# stsci-aws-utils
Utilities for running STScI software on Amazon Web Services.

## S3

The `ConcurrentS3Client` class streams S3 objects into memory (BytesIO)
using multiple concurrent HTTP range requests.  In tests on an m4.10xlarge
with a 10 Gpbs network connection, we have seen ~ 3x speedup compared to
boto3's S3 client.  Client credentials and region are configured as for
boto3 (environment variables, .aws directory, or instance metadata).

`AsyncConcurrentS3Client` is similar, but with an async interface.

An example of opening a FITS file from S3 using `ConcurrentS3Client`:

```python
from stsci_aws_utils.s3 import ConcurrentS3Client
from astropy.io import fits

with ConcurrentS3Client() as client:
    content = client.get_object("some-s3-bucket", "some/key/prefix/some-file.fits")
    hdul = fits.open(content)
```

Note that when making successive requests, it is more efficient to reuse an open
client than to create a new one every time.  It is possible to create the client
without context management, but you'll need to remember to call the `close()`
method yourself:

```python
client = ConcurrentS3Client()

# ...

client.close()
```

Otherwise, you'll see a cryptic complaint written to stderr by aiohttp.

If you find yourself in a situation where it's inconvenient to close the client
(maybe you're implementing a module method), `atexit` is at your service:

```python
import atexit

CLIENT = ConcurrentS3Client()
atexit.register(CLIENT.close)
```

It ensures that close is called before normal program exit.