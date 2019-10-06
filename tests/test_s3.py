import asyncio
import os
import math
from io import BytesIO

import pytest
import aiohttp
import hashlib

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
        for i in range(s3._DEFAULT_MAX_ATTEMPTS):
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

    async def get_object(self, bucket_name, key):
        return BytesIO()

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

    def test_close(self, client):
        assert client._async_client.closed is False
        client.close()
        assert client._async_client.closed is True

    def test_context_management(self):
        with s3.ConcurrentS3Client(async_client_class=MockAsyncClient) as client:
            async_client = client._async_client
            assert async_client.closed is False
        assert async_client.closed is True
