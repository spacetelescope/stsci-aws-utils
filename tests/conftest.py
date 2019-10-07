import socket
import asyncio
import os

import pytest
import aiohttp

from .certificate import TemporaryCertificate


S3_BUCKET_NAME = "bucket-of-test-data"
S3_PORT = 443
S3_HOSTNAME = f"{S3_BUCKET_NAME}.s3.amazonaws.com"
AWS_ACCESS_KEY_ID = "AAAACCCCCCEEESSSSSSS"
AWS_SECRET_ACCESS_KEY = "ssssseeeeeeccccccrrrrrreeeeeeetttttttttt"
AWS_DEFAULT_REGION = "gl-east-12"


os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
os.environ["AWS_DEFAULT_REGION"] = AWS_DEFAULT_REGION


# Courtesy of https://solidabstractions.com/2018/testing-aiohttp-client
class TestResolver(aiohttp.abc.AbstractResolver):
    def __init__(self):
        self._servers = {}

    def add(self, host, port, target_port):
        self._servers[host, port] = target_port

    async def resolve(self, host, port=0, family=socket.AF_INET):
        try:
            fake_port = self._servers[host, port]
        except KeyError:
            raise OSError(f"No test server known for {host}")
        return [
            {
                "hostname": host,
                "host": "127.0.0.1",
                "port": fake_port,
                "family": socket.AF_INET,
                "proto": 0,
                "flags": socket.AI_NUMERICHOST,
            }
        ]

    async def close(self):
        pass


# Courtesy of https://solidabstractions.com/2018/testing-aiohttp-client
class TestServer(aiohttp.test_utils.RawTestServer):
    def __init__(self, *, ssl=None, **kwargs):
        super().__init__(self._handle_request, **kwargs)
        self._ssl = ssl
        self._requests = asyncio.Queue()
        self._responses = {}

    async def start_server(self, **kwargs):
        kwargs.setdefault("ssl", self._ssl)
        await super().start_server(**kwargs)

    async def close(self):
        for future in self._responses.values():
            future.cancel()
        await super().close()

    async def _handle_request(self, request):
        self._responses[id(request)] = response = asyncio.Future()
        self._requests.put_nowait(request)
        try:
            return await response
        finally:
            del self._responses[id(request)]

    async def receive_request(self):
        return await self._requests.get()

    def send_response(self, request, *args, **kwargs):
        response = aiohttp.web.Response(*args, **kwargs)
        self._responses[id(request)].set_result(response)


# Courtesy of https://solidabstractions.com/2018/testing-aiohttp-client-part-2
@pytest.fixture(scope="session")
def ssl_certificate():
    with TemporaryCertificate() as certificate:
        yield certificate


@pytest.fixture
async def server(ssl_certificate):
    async with TestServer(ssl=ssl_certificate.server_context()) as server:
        yield server


@pytest.fixture
def s3_resolver(server):
    resolver = TestResolver()
    resolver.add(S3_HOSTNAME, S3_PORT, server.port)
    return resolver


@pytest.fixture
async def s3_session(s3_resolver, ssl_certificate):
    connector = aiohttp.TCPConnector(resolver=s3_resolver, ssl=ssl_certificate.client_context(), use_dns_cache=False)
    async with aiohttp.ClientSession(connector=connector, raise_for_status=True) as session:
        yield session
