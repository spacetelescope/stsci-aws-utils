import asyncio
import os
import math
from io import BytesIO
from xml.etree import ElementTree

import pytest
import hashlib
import aiohttp

from stsci_aws_utils import s3

from . import conftest


@pytest.fixture(autouse=True)
def monkeypatch_retry_constants(monkeypatch):
    # No need to sleep while tests are running
    monkeypatch.setattr(s3, "_BACKOFF_CAP", 0.1)
    monkeypatch.setattr(s3, "_MIN_DOWNLOAD_PART_SIZE_BYTES", 1024)


def generate_object_data(content_length, num_parts):
    content = os.urandom(content_length)

    if num_parts == 1:
        etag = '"' + compute_md5(content).hex() + '"'
    else:
        assert content_length > (num_parts * 1024 * 1024)

        part_size_bytes = math.floor(content_length / (1024 * 1024) / (num_parts - 1)) * 1024 * 1024
        partial_md5s = []
        start_byte = 0
        while start_byte < content_length:
            end_byte = min(content_length, start_byte + part_size_bytes)
            partial_md5s.append(compute_md5(content[start_byte:end_byte]))
            start_byte = end_byte
        etag = '"' + compute_md5(b"".join(partial_md5s)).hex() + f"-{num_parts}" + '"'

    return etag, content


def create_root_elem(tag):
    return ElementTree.Element(tag, {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})


def create_contents_elem(root_elem, key):
    contents_elem = ElementTree.SubElement(root_elem, "Contents")
    key_elem = ElementTree.SubElement(contents_elem, "Key")
    key_elem.text = key
    return contents_elem


def create_next_continuation_token_elem(root_elem, token):
    next_continuation_token_elem = ElementTree.SubElement(root_elem, "NextContinuationToken")
    next_continuation_token_elem.text = token
    return next_continuation_token_elem


async def consume_generator(gen):
    results = []
    async for result in gen:
        results.append(result)
    return results


def compute_md5(value):
    return hashlib.md5(value).digest()


async def assert_head_and_respond(server, key, status=200, content_length=None, etag=None):
    request = await server.receive_request()
    assert request.path_qs == "/" + key
    assert request.method == "HEAD"
    headers = {}
    if content_length:
        headers["Content-Length"] = str(content_length)
    if etag:
        headers["Etag"] = etag
    server.send_response(request, status=status, headers=headers)


async def assert_get_and_respond(server, key, content, status_callback=None, status=200):
    request = await server.receive_request()
    assert request.path_qs == "/" + key
    assert request.method == "GET"
    start_byte, end_byte = [int(b) for b in request.headers["Range"].split("=")[-1].split("-")]
    assert start_byte >= 0 and start_byte < len(content)
    assert end_byte > start_byte and end_byte >= 0 and end_byte < len(content)

    if status_callback is not None:
        status = status_callback(request)

    if status != 200:
        server.send_response(request, status=status)
    else:
        server.send_response(request, status=status, body=content[start_byte : end_byte + 1])


async def assert_get_and_respond_xml(server, path, tree, status_callback=None, status=200):
    request = await server.receive_request()
    assert request.path_qs == path
    assert request.method == "GET"

    if status_callback is not None:
        status = status_callback(request)

    if status != 200:
        server.send_response(request, status=status)
    else:
        content = ElementTree.tostring(tree, encoding="utf8", method="xml")
        server.send_response(request, status=status, body=content)


class TestAsyncConcurrentS3Client:
    @pytest.fixture
    def client(self, s3_session):
        return s3.AsyncConcurrentS3Client(session=s3_session)

    @pytest.mark.asyncio
    async def test_object_exists_missing(self, client, server):
        task = asyncio.ensure_future(client.object_exists(conftest.S3_BUCKET_NAME, "missing/key"))
        await assert_head_and_respond(server, "missing/key", status=404)
        result = await task
        assert result is False

    @pytest.mark.asyncio
    async def test_object_exists_present(self, client, server):
        task = asyncio.ensure_future(client.object_exists(conftest.S3_BUCKET_NAME, "present/key"))
        await assert_head_and_respond(server, "present/key")
        result = await task
        assert result is True

    @pytest.mark.asyncio
    async def test_object_exists_retry_success(self, client, server):
        task = asyncio.ensure_future(client.object_exists(conftest.S3_BUCKET_NAME, "flaky/key"))
        await assert_head_and_respond(server, "flaky/key", status=500)
        await assert_head_and_respond(server, "flaky/key")
        result = await task
        assert result is True

    @pytest.mark.asyncio
    async def test_object_exists_retry_failure(self, client, server):
        task = asyncio.ensure_future(client.object_exists(conftest.S3_BUCKET_NAME, "flaky/key"))
        for i in range(s3._DEFAULT_MAX_ATTEMPTS):
            await assert_head_and_respond(server, "flaky/key", status=500)
        with pytest.raises(aiohttp.ClientResponseError):
            await task

    @pytest.mark.asyncio
    async def test_object_exists_client_error(self, client, server):
        task = asyncio.ensure_future(client.object_exists(conftest.S3_BUCKET_NAME, "impermissible/key"))
        await assert_head_and_respond(server, "impermissible/key", status=400)
        with pytest.raises(aiohttp.ClientResponseError):
            await task

    @pytest.mark.asyncio
    async def test_get_object_single_request(self, client, server):
        content_length = 1024
        etag, content = generate_object_data(content_length, 1)
        task = asyncio.ensure_future(client.get_object(conftest.S3_BUCKET_NAME, "small/object/key"))
        await assert_head_and_respond(server, "small/object/key", content_length=content_length, etag=etag)
        await assert_get_and_respond(server, "small/object/key", content)
        result = await task
        assert result.getbuffer() == content

    @pytest.mark.asyncio
    async def test_get_object_multiple_request(self, client, server):
        content_length = s3._MIN_DOWNLOAD_PART_SIZE_BYTES * (s3._DEFAULT_MAX_CONCURRENT_REQUESTS + 1) + 1023
        etag, content = generate_object_data(content_length, 1)
        task = asyncio.ensure_future(client.get_object(conftest.S3_BUCKET_NAME, "larger/object/key"))
        await assert_head_and_respond(server, "larger/object/key", content_length=content_length, etag=etag)
        for _ in range(s3._DEFAULT_MAX_CONCURRENT_REQUESTS):
            await assert_get_and_respond(server, "larger/object/key", content)
        result = await task
        assert result.getbuffer() == content

    @pytest.mark.asyncio
    async def test_get_object_multipart_md5(self, client, server):
        content_length = 1024 * 1024 * (s3._DEFAULT_MAX_CONCURRENT_REQUESTS + 1) + 1023
        etag, content = generate_object_data(content_length, 3)
        task = asyncio.ensure_future(client.get_object(conftest.S3_BUCKET_NAME, "multipart/object/key"))
        await assert_head_and_respond(server, "multipart/object/key", content_length=content_length, etag=etag)
        for _ in range(s3._DEFAULT_MAX_CONCURRENT_REQUESTS):
            await assert_get_and_respond(server, "multipart/object/key", content)
        result = await task
        assert result.getbuffer() == content

    @pytest.mark.asyncio
    async def test_get_object_md5_failure(self, client, server):
        content_length = 1024
        _, content = generate_object_data(content_length, 1)
        etag = "nope"
        task = asyncio.ensure_future(client.get_object(conftest.S3_BUCKET_NAME, "small/object/key"))
        await assert_head_and_respond(server, "small/object/key", content_length=content_length, etag=etag)
        await assert_get_and_respond(server, "small/object/key", content)
        with pytest.raises(aiohttp.ClientError):
            await task

    @pytest.mark.asyncio
    async def test_get_object_head_retry_success(self, client, server):
        content_length = 1024
        etag, content = generate_object_data(content_length, 1)
        task = asyncio.ensure_future(client.get_object(conftest.S3_BUCKET_NAME, "flaky/key"))
        await assert_head_and_respond(server, "flaky/key", status=500)
        await assert_head_and_respond(server, "flaky/key", content_length=content_length, etag=etag)
        await assert_get_and_respond(server, "flaky/key", content)
        result = await task
        assert result.getbuffer() == content

    @pytest.mark.asyncio
    async def test_get_object_head_retry_failure(self, client, server):
        task = asyncio.ensure_future(client.get_object(conftest.S3_BUCKET_NAME, "flaky/key"))
        for _ in range(s3._DEFAULT_MAX_ATTEMPTS):
            await assert_head_and_respond(server, "flaky/key", status=500)
        with pytest.raises(aiohttp.ClientResponseError):
            await task

    @pytest.mark.asyncio
    async def test_get_object_request_retry_success(self, client, server):
        content_length = s3._MIN_DOWNLOAD_PART_SIZE_BYTES * (s3._DEFAULT_MAX_CONCURRENT_REQUESTS + 1) + 1023
        etag, content = generate_object_data(content_length, 1)
        task = asyncio.ensure_future(client.get_object(conftest.S3_BUCKET_NAME, "larger/object/key"))
        await assert_head_and_respond(server, "larger/object/key", content_length=content_length, etag=etag)
        failed = False

        def status_callback(request):
            nonlocal failed
            if not failed:
                failed = True
                return 500
            else:
                return 200

        for _ in range(s3._DEFAULT_MAX_CONCURRENT_REQUESTS + 1):
            await assert_get_and_respond(server, "larger/object/key", content, status_callback=status_callback)
        result = await task
        assert result.getbuffer() == content

    @pytest.mark.asyncio
    async def test_get_object_request_retry_failure(self, client, server):
        content_length = s3._MIN_DOWNLOAD_PART_SIZE_BYTES * (s3._DEFAULT_MAX_CONCURRENT_REQUESTS + 1) + 1023
        etag, content = generate_object_data(content_length, 1)
        task = asyncio.ensure_future(client.get_object(conftest.S3_BUCKET_NAME, "larger/object/key"))
        await assert_head_and_respond(server, "larger/object/key", content_length=content_length, etag=etag)
        await assert_get_and_respond(server, "larger/object/key", content, status=400)
        with pytest.raises(aiohttp.ClientResponseError):
            await task

    @pytest.mark.asyncio
    async def test_prefix_exists(self, client, server):
        root_elem = create_root_elem("ListBucketResult")
        task = asyncio.ensure_future(client.prefix_exists(conftest.S3_BUCKET_NAME, "missing/prefix/"))
        await assert_get_and_respond_xml(server, "/?list-type=2&Prefix=missing/prefix/", root_elem)
        result = await task
        assert result is False

        create_contents_elem(root_elem, "present/prefix/file.dat")
        task = asyncio.ensure_future(client.prefix_exists(conftest.S3_BUCKET_NAME, "present/prefix/"))
        await assert_get_and_respond_xml(server, "/?list-type=2&Prefix=present/prefix/", root_elem)
        result = await task
        assert result is True

    @pytest.mark.asyncio
    async def test_iterate_keys(self, client, server):
        root_elem = create_root_elem("ListBucketResult")
        task = asyncio.ensure_future(consume_generator(client.iterate_keys(conftest.S3_BUCKET_NAME, "missing/prefix/")))
        await assert_get_and_respond_xml(server, "/?list-type=2&Prefix=missing/prefix/", root_elem)
        result = await task
        assert len(result) == 0

        create_contents_elem(root_elem, "present/prefix/file1.dat")
        create_contents_elem(root_elem, "present/prefix/file2.dat")
        task = asyncio.ensure_future(consume_generator(client.iterate_keys(conftest.S3_BUCKET_NAME, "present/prefix/")))
        await assert_get_and_respond_xml(server, "/?list-type=2&Prefix=present/prefix/", root_elem)
        result = await task
        assert len(result) == 2
        assert result[0] == "present/prefix/file1.dat"
        assert result[1] == "present/prefix/file2.dat"

    @pytest.mark.asyncio
    async def test_iterate_keys_no_prefix(self, client, server):
        root_elem = create_root_elem("ListBucketResult")
        create_contents_elem(root_elem, "file1.dat")
        create_contents_elem(root_elem, "file2.dat")
        task = asyncio.ensure_future(consume_generator(client.iterate_keys(conftest.S3_BUCKET_NAME)))
        await assert_get_and_respond_xml(server, "/?list-type=2", root_elem)
        result = await task
        assert len(result) == 2
        assert result[0] == "file1.dat"
        assert result[1] == "file2.dat"

    @pytest.mark.asyncio
    async def test_iterate_keys_multiple_requests(self, client, server):
        keys = [f"prefix/file{i}.dat" for i in range(6)]

        first_root_elem = create_root_elem("ListBucketResult")
        for key in keys[0:2]:
            create_contents_elem(first_root_elem, key)
        create_next_continuation_token_elem(first_root_elem, "first-continuation-token")

        second_root_elem = create_root_elem("ListBucketResult")
        for key in keys[2:4]:
            create_contents_elem(second_root_elem, key)
        create_next_continuation_token_elem(second_root_elem, "second-continuation-token")

        third_root_elem = create_root_elem("ListBucketResult")
        for key in keys[4:6]:
            create_contents_elem(third_root_elem, key)

        task = asyncio.ensure_future(consume_generator(client.iterate_keys(conftest.S3_BUCKET_NAME, "prefix/")))
        await assert_get_and_respond_xml(server, "/?list-type=2&Prefix=prefix/", first_root_elem)
        await assert_get_and_respond_xml(
            server, "/?list-type=2&Prefix=prefix/&ContinuationToken=first-continuation-token", second_root_elem
        )
        await assert_get_and_respond_xml(
            server, "/?list-type=2&Prefix=prefix/&ContinuationToken=second-continuation-token", third_root_elem
        )

        result = await task
        assert result == keys

    @pytest.mark.asyncio
    async def test_close(self, client, s3_session):
        assert s3_session.closed is False
        await client.close()
        assert s3_session.closed is True

    @pytest.mark.asyncio
    async def test_context_management(self, s3_session):
        assert s3_session.closed is False
        async with s3.AsyncConcurrentS3Client(session=s3_session):
            assert s3_session.closed is False
        assert s3_session.closed is True


class MockAsyncClient:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.closed = False

    async def object_exists(self, bucket_name, key):
        return True

    async def prefix_exists(self, bucket_name, key_prefix):
        return False

    async def get_object(self, bucket_name, key):
        return BytesIO()

    async def _list_keys(self, bucket_name, key_prefix, continuation_token):
        if continuation_token is None:
            return "some-continuation-token", ["file1.dat", "file2.dat"]
        elif continuation_token == "some-continuation-token":
            return None, ["file3.dat", "file4.dat"]
        else:
            raise ValueError("Unexpected continuation_token")

    async def close(self):
        self.closed = True


class TestConcurrentS3Client:
    @pytest.fixture
    def client(self):
        return s3.ConcurrentS3Client(async_client_class=MockAsyncClient)

    def test_object_exists(self, client):
        assert client.object_exists(conftest.S3_BUCKET_NAME, "some/key") is True

    def test_get_object(self, client):
        assert isinstance(client.get_object(conftest.S3_BUCKET_NAME, "some/key"), BytesIO)

    def test_prefix_exists(self, client):
        assert client.prefix_exists(conftest.S3_BUCKET_NAME, "some/prefix/") is False

    def test_iterate_keys(self, client):
        result = [k for k in client.iterate_keys(conftest.S3_BUCKET_NAME, "some/prefix/")]
        assert result == ["file1.dat", "file2.dat", "file3.dat", "file4.dat"]

    def test_close(self, client):
        assert client._async_client.closed is False
        client.close()
        assert client._async_client.closed is True

    def test_context_management(self):
        with s3.ConcurrentS3Client(async_client_class=MockAsyncClient) as client:
            async_client = client._async_client
            assert async_client.closed is False
        assert async_client.closed is True
