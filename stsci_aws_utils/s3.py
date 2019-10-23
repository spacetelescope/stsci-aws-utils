import asyncio
from io import BytesIO
import logging
import random
import math
import hashlib
from typing import Any
from defusedxml import ElementTree

import aiohttp
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import Session


__all__ = ["ConcurrentS3Client", "AsyncConcurrentS3Client"]


_LOGGER = logging.getLogger(__name__)

# Experiments on an EC2 instance with a 10 Gbps network connection
# show diminishing returns (and sometimes increases in total time)
# with more than 6 concurrent requests.
_DEFAULT_MAX_CONCURRENT_REQUESTS = 6
# This matches the default max attempts defined in botocore.
_DEFAULT_MAX_ATTEMPTS = 5
# By default, don't timeout.
_DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=None, sock_connect=None, sock_read=None)

# Don't bother making concurrent requests unless the object
# exceeds this size.
_MIN_DOWNLOAD_PART_SIZE_BYTES = 5 * 1024 * 1024
# The minimum time (jitter notwithstanding) to sleep after
# the first failure.  This matches the base defined in botocore
# for S3.
_BACKOFF_BASE = 0.5
# The maximum sleep time between failed requests.
_BACKOFF_CAP = 15
# The size of the buffer that we read into when computing
# MD5 checksums.
_MD5_CHUNK_SIZE_BYTES = 1024 * 1024
_XML_NAMESPACES = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
# These are factors of 1 MiB that are used to divide multipart object
# content into parts for MD5 computation.  It is not currently known
# if these cover all of aws-cli's behavior.  If you're experiencing
# mysterious MD5 checksum failures, try adding additional powers of 2
# here.
_MULTIPART_MD5_FACTORS = [1, 2]


class AsyncConcurrentS3Client:
    """
    Async S3 client that streams large objects into memory (BytesIO) using
    multiple concurrent HTTP range requests.

    Parameters
    ----------
    max_concurrent_requests : int, optional
        Maximum number of requests used to download the object content.
        Defaults to 6.
    max_attempts : int, optional
        Maximum number of attempts made per individual request.  Connection errors
        or 5xx errors from S3 will be retried.  Client errors (4xx) except 404 and
        timeout errors will not be retried.  Defaults to 5.
    timeout : aiohttp.ClientTimeout, optional
        aiohttp timeout configuration used for requests.  Defaults to no timeout.
    session : aiohttp.ClientSession, optional
        aiohttp session object used to make requests.  If you provide a custom session,
        be sure to create it with raise_for_status=True.
   """

    def __init__(
        self,
        *,
        max_concurrent_requests: int = _DEFAULT_MAX_CONCURRENT_REQUESTS,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
        timeout: aiohttp.ClientTimeout = _DEFAULT_TIMEOUT,
        session: aiohttp.ClientSession = None,
    ):
        self._max_concurrent_requests = max_concurrent_requests
        self._max_attempts = max_attempts

        # Fetch the credentials and default region from botocore's session.
        # This will automatically find configuration in the user's .aws folder,
        # or in instance metadata.
        boto_session = Session()
        self._credentials = boto_session.get_credentials()
        self._region = boto_session.get_config_variable("region")

        if session is None:
            self._session = aiohttp.ClientSession(raise_for_status=True, timeout=timeout)
        else:
            self._session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def get_object(self, bucket_name: str, key: str):
        """
        Fetch the content of an object from S3.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket that contains the object.
        key : str
            Key of the object.

        Returns
        -------
        io.BytesIO
            The content of the object.

        Raises
        ------
        aiohttp.ClientError
            On request failure, connection error, or MD5 checksum
            failure (after retries).
        asyncio.TimeoutError
            On request timeout.
        """

        url = self._make_object_url(bucket_name, key)
        headers = await self._request_with_retries(self._head_object, url)
        content_length = int(headers["Content-Length"])

        # For objects that were uploaded to S3 in a single request, the
        # Etag header contains the hex-encoded MD5 hash of the object's
        # content.  For objects that were uploaded using the multipart
        # API, Etag contains the hex-encoded MD5 hash of the concatenated
        # MD5 hashes of each part, plus the number of parts, joined by
        # a hyphen.
        etag_parts = headers["Etag"].strip('"').split("-")
        expected_md5 = etag_parts[0]
        if len(etag_parts) > 1:
            num_parts = int(etag_parts[-1])
        else:
            num_parts = 1

        # Even though we already know the content length at this point, we don't
        # size the BytesIO just yet.  Allocating memory for large objects takes
        # significant time, so we should wait until the HTTP requests have been
        # made.  The BytesIO is sized in _get_object_part below.
        content = BytesIO()

        # Break the download into separate requests, but don't bother if the content
        # length is <= _MIN_DOWNLOAD_PART_SIZE_BYTES.
        bytes_per_part = max(math.ceil(content_length / self._max_concurrent_requests), _MIN_DOWNLOAD_PART_SIZE_BYTES)
        tasks = []
        next_byte = 0
        while next_byte < content_length:
            end_byte = min(content_length, next_byte + bytes_per_part)
            tasks.append(
                asyncio.ensure_future(
                    self._request_with_retries(self._get_object_part, url, next_byte, end_byte, content, content_length)
                )
            )
            next_byte = end_byte

        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            # asyncio.gather(...) will stop at first task exception, but it won't
            # automatically cancel the other tasks.
            for task in tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        _LOGGER.warning("Exception raised cancelling task", exc_info=True)
            raise e

        # If we see this happen in the wild, we should consider adding
        # an overall retry.
        for min_part_factor in _MULTIPART_MD5_FACTORS:
            md5 = self._compute_md5(content, content_length, num_parts, min_part_factor).hex()
            if md5 == expected_md5:
                break
        if md5 != expected_md5:
            raise aiohttp.ClientError("Failed MD5 checksum verification")

        content.seek(0)
        return content

    def _compute_md5(self, file, content_length, num_parts=1, min_part_factor=1):
        if num_parts == 1:
            # Single-part uploads have a simple MD5 of their content.
            return self._compute_partial_md5(file, 0, content_length)
        else:
            # AWS computes multipart MD5 by concatenating the MD5s of each part
            # and hashing that whole thing again.  We don't actually know the
            # size of each part at this point, but we can make a reasonable
            # guess with some assumptions: we assume here that all parts but the
            # last are the same size, and we assume that the part size is a
            # multiple of min_part_factor * 1024 * 1024.
            part_size_bytes = (
                math.floor(content_length / (min_part_factor * 1024 * 1024) / (num_parts - 1))
                * min_part_factor
                * 1024
                * 1024
            )
            partial_md5s = []
            start_byte = 0
            while start_byte < content_length:
                end_byte = min(content_length, start_byte + part_size_bytes)
                partial_md5s.append(self._compute_partial_md5(file, start_byte, end_byte))
                start_byte = end_byte
            return hashlib.md5(b"".join(partial_md5s)).digest()  # nosec

    def _compute_partial_md5(self, file, start_byte, end_byte):
        # Compute an MD5 checksum for the specified byte range of the file.
        md5 = hashlib.md5()  # nosec

        if end_byte > start_byte:
            file.seek(start_byte)
            current_byte = start_byte
            # Use a bytearray here to avoid allocating an entire file's worth of small
            # bytes objects.
            mv = memoryview(bytearray(min(_MD5_CHUNK_SIZE_BYTES, end_byte - start_byte)))
            while current_byte < end_byte:
                read_size = file.readinto(mv)
                md5.update(mv[0 : min(read_size, end_byte - current_byte)])
                current_byte += read_size

        return md5.digest()

    async def object_exists(self, bucket_name: str, key: str):
        """
        Determine if an object exists on S3.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket that contains the object.
        key : str
            Key of the object.

        Returns
        -------
        bool
            `True` if object exists, `False` if not.

        Raises
        ------
        aiohttp.ClientError
            On request failure or connection error (after retries).
        """
        url = self._make_object_url(bucket_name, key)
        try:
            await self._request_with_retries(self._head_object, url)
            # If the HEAD request succeeds, then the object exists.
            return True
        except aiohttp.ClientResponseError as e:
            # Not all client errors indicate a missing object -- we could
            # be lacking permissions, for example.  Re-raise non-404 errors
            # so that the user knows there is a real problem.
            if e.status == 404:
                return False
            else:
                raise e

    async def prefix_exists(self, bucket_name: str, key_prefix: str):
        """
        Determine if any objects exist on S3 in the specified bucket
        and key prefix.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket that contains the object.
        key_prefix : str
            Key prefix of the objects.

        Returns
        -------
        bool
            `True` if objects exist, `False` if not.

        Raises
        ------
        aiohttp.ClientError
            On request failure or connection error (after retries).
        """
        found = False
        async for _ in self.iterate_keys(bucket_name, key_prefix):
            found = True
            break
        return found

    async def iterate_keys(self, bucket_name: str, key_prefix: str = None):
        """
        Iterate bucket keys, optionally with the specified prefix.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket to query.
        key_prefix : str, optional
            Key prefix of the objects.

        Yields
        ------
        str
            Key of an object in the bucket.

        Raises
        ------
        aiohttp.ClientError
            On request failure or connection error (after retries).
        """
        next_continuation_token = None
        while True:
            next_continuation_token, keys = await self._list_keys(bucket_name, key_prefix, next_continuation_token)
            for key in keys:
                yield key
            if next_continuation_token is None:
                break

    async def _list_keys(self, bucket_name, key_prefix, continuation_token):
        url = self._make_list_objects_url(bucket_name, prefix=key_prefix, continuation_token=continuation_token)
        root = await self._request_with_retries(self._get_xml_response, url)
        keys = [
            elem.findtext("s3:Key", namespaces=_XML_NAMESPACES)
            for elem in root.iterfind("s3:Contents", _XML_NAMESPACES)
        ]
        return root.findtext("s3:NextContinuationToken", namespaces=_XML_NAMESPACES), keys

    async def _get_xml_response(self, url):
        headers = self._sign_headers("GET", url)
        async with self._session.get(url, headers=headers) as response:
            content = await response.read()
            return ElementTree.fromstring(content)

    async def _head_object(self, url):
        headers = self._sign_headers("HEAD", url)
        async with self._session.head(url, headers=headers) as response:
            return response.headers

    async def _get_object_part(self, url, start_byte, end_byte, content, content_length):
        # HTTP range includes the end byte.
        headers = {"Range": f"bytes={start_byte}-{end_byte - 1}"}
        headers = self._sign_headers("GET", url, headers)
        async with self._session.get(url, headers=headers) as response:
            current_byte = start_byte
            # Iterate over response chunks as they become available, in whatever
            # size they appear.  This prevents aiohttp from doing any unneccessary
            # concatenation of the bytes objects.
            async for chunk in response.content.iter_any():
                # The only time the BytesIO position will be 0 is before any
                # data has been written.  Now we'll size it by making a dummy
                # write at the end.
                if content.tell() == 0:
                    content.seek(content_length - 1)
                    content.write(b"0")
                content.seek(current_byte)
                content.write(chunk)
                current_byte += len(chunk)

    def _make_object_url(self, bucket_name, key):
        return f"https://{bucket_name}.s3.amazonaws.com/" + key

    def _make_list_objects_url(self, bucket_name, prefix=None, continuation_token=None):
        url = f"https://{bucket_name}.s3.amazonaws.com/?list-type=2"
        if prefix is not None:
            url = url + f"&Prefix={prefix}"
        if continuation_token is not None:
            url = url + f"&ContinuationToken={continuation_token}"
        return url

    def _sign_headers(self, method, url, headers={}):
        # Hijack botocore's machinery to sign the request.  This is a complicated
        # process that involves reformatting the request data according to specific
        # rules, and we don't want to implement that ourselves.
        # This code will need to be modified if we ever need to send requests
        # with data.
        request = AWSRequest(method=method, url=url, headers=headers)
        S3SigV4Auth(self._credentials, "s3", self._region).add_auth(request)
        return dict(request.headers.items())

    async def _request_with_retries(self, request_method, *args, **kwargs):
        attempt = 1
        while True:
            try:
                return await request_method(*args, **kwargs)
            except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, aiohttp.ClientResponseError) as e:
                # For S3, it is usually fruitless to retry 4xx errors, since they indicate a problem
                # that requires intervention to fix (such as missing objects or permissions errors).
                if attempt >= self._max_attempts or (isinstance(e, aiohttp.ClientResponseError) and e.status < 500):
                    raise e
                else:
                    _LOGGER.warning("S3 request failed, will retry", exc_info=True)
                    # This is "exponential backoff with full jitter", per
                    # https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
                    sleep_time = random.uniform(0, min(_BACKOFF_CAP, _BACKOFF_BASE * 2 ** (attempt - 1)))  # nosec
                    await asyncio.sleep(sleep_time)
                    attempt += 1

    async def close(self):
        """
        Release the underlying HTTP client's resources.
        """

        # If we don't do this, we'll get shamed by a message to stderr
        # from aiohttp.
        await self._session.close()


class ConcurrentS3Client:
    """
    Sync wrapper for AsyncConcurrentS3Client.  Streams large S3 objects into
    memory (BytesIO) using multiple concurrent HTTP range requests.  Uses the
    default event loop to execute the requests.

        Parameters
    ----------
    max_concurrent_requests : int, optional
        Maximum number of requests used to download the object content.
        Defaults to 6.
    max_attempts : int, optional
        Maximum number of attempts made per individual request.  Connection errors
        or 5xx errors from S3 will be retried.  Client errors (4xx) except 404 and
        timeout errors will not be retried.  Defaults to 5.
    timeout : aiohttp.ClientTimeout, optional
        aiohttp timeout configuration used for requests.  Defaults to no timeout.
    session : aiohttp.ClientSession, optional
        aiohttp session object used to make requests.  If you provide a custom session,
        be sure to create it with raise_for_status=True.
    """

    def __init__(
        self,
        *,
        max_concurrent_requests: int = _DEFAULT_MAX_CONCURRENT_REQUESTS,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
        timeout: aiohttp.ClientTimeout = _DEFAULT_TIMEOUT,
        session: aiohttp.ClientSession = None,
        async_client_class: Any = AsyncConcurrentS3Client,
    ):
        self._loop = asyncio.get_event_loop()
        self._async_client = async_client_class(
            max_concurrent_requests=max_concurrent_requests, max_attempts=max_attempts, timeout=timeout, session=session
        )

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def get_object(self, bucket_name: str, key: str):
        """
        Fetch the content of an object from S3.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket that contains the object.
        key : str
            Key of the object.

        Returns
        -------
        io.BytesIO
            The content of the object.

        Raises
        ------
        aiohttp.ClientError
            On request failure, connection error, or MD5 checksum
            failure (after retries).
        asyncio.TimeoutError
            On request timeout.
        """

        return self._loop.run_until_complete(self._async_client.get_object(bucket_name, key))

    def object_exists(self, bucket_name: str, key: str):
        """
        Determine if an object exists on S3.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket that contains the object.
        key : str
            Key of the object.

        Returns
        -------
        bool
            `True` if object exists, `False` if not.

        Raises
        ------
        aiohttp.ClientError
            On request failure or connection error (after retries).
        """

        return self._loop.run_until_complete(self._async_client.object_exists(bucket_name, key))

    def prefix_exists(self, bucket_name: str, key_prefix: str):
        """
        Determine if any objects exist on S3 in the specified bucket
        and key prefix.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket that contains the object.
        key_prefix : str
            Key prefix of the objects.

        Returns
        -------
        bool
            `True` if objects exist, `False` if not.

        Raises
        ------
        aiohttp.ClientError
            On request failure or connection error (after retries).
        """

        return self._loop.run_until_complete(self._async_client.prefix_exists(bucket_name, key_prefix))

    def iterate_keys(self, bucket_name: str, key_prefix: str = None):
        """
        Iterate bucket keys, optionally with the specified prefix.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket to query.
        key_prefix : str, optional
            Key prefix of the objects.

        Yields
        ------
        str
            Key of an object in the bucket.

        Raises
        ------
        aiohttp.ClientError
            On request failure or connection error (after retries).
        """
        next_continuation_token = None
        while True:
            next_continuation_token, keys = self._loop.run_until_complete(
                self._async_client._list_keys(bucket_name, key_prefix, next_continuation_token)
            )
            yield from keys
            if next_continuation_token is None:
                break

    def close(self):
        """
        Release the underlying HTTP client's resources.
        """

        self._loop.run_until_complete(self._async_client.close())
