"""
Defines a series of classes for reading files from S3
"""

import asyncio
import cgi
import concurrent
import logging
import re


def parse_url(url):
    match = re.match("s3://([a-z0-9,.-]+)/(.*)", url)
    return match.group(1, 2) if match else None

def open_file(client, bucket, key):
    """ Retrieves a stream and the charset for the file on S3 """
    logger = logging.getLogger(__name__)
    logger.info("Reading S3 file: %s", key)

    response = client.get_object(Bucket=bucket, Key=key)
    _, type_params = cgi.parse_header(response["ContentType"])

    stream = response["Body"]
    charset = type_params.get("charset", None)

    return stream, charset

def open_reader(client, bucket, key, loop=None, **kwargs):
    """ Opens a file in S3 and returns a stream """
    stream = S3Stream.open(client, bucket, key)
    reader = asyncio.StreamReader()

    transport = S3ReadTransport(reader, stream, loop=loop, **kwargs)

    reader.set_transport(transport)
    return reader, transport


class S3ReadTransport(asyncio.ReadTransport):
    """ Custom asyncio.ReadTransport for reading from a StreamingResponse """

    DEFAULT_BUFFER_AMOUNT = 65536

    get_protocol = set_protocol = None

    def __init__(self, reader, stream, loop=None, **kwargs):
        super(S3ReadTransport, self).__init__()
        self.logger = logging.getLogger(__name__)

        self.reader = reader
        self.stream = stream

        self.buffer_amount = kwargs.get("buffer_amount", S3ReadTransport.DEFAULT_BUFFER_AMOUNT)
        self.task = asyncio.ensure_future(self._process_safely(), loop=loop)
        self.waiter = asyncio.Event(loop=loop)

        self.resume_reading()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def _process_stream(self):
        while not self.stream.at_eof:
            await self.waiter.wait()

            data = self.stream.read(self.buffer_amount)

            self.reader.feed_data(data)

    async def _process_safely(self):
        try:
            await self._process_stream()
        except concurrent.futures.CancelledError:
            raise
        except Exception:  # pylint: disable=broad-except
            self.logger.exception("Failed reading from S3")
        finally:
            self.reader.feed_eof()
            self.stream.close()

    def close(self):
        self.task.cancel()

    def get_encoding(self):
        return self.stream.encoding

    def is_closing(self):
        self.task.done()

    def pause_reading(self):
        self.waiter.clear()

    def resume_reading(self):
        self.waiter.set()


class S3Stream(object):
    """ Custom asyncio.ReadTransport for reading from a StreamingResponse """

    DEFAULT_ENCODING = "utf-8"

    def __init__(self, stream, encoding):
        self.logger = logging.getLogger(__name__)

        self.encoding = encoding
        self.stream = stream

        self.at_eof = False

    @classmethod
    def open(cls, client, bucket, key):
        """ Creates an S3Stream for the given file on S3 """
        return cls(*open_file(client, bucket, key))

    def close(self):
        self.stream.close()

    def read(self, amount):
        """ Reads a chunk of data from the S3 file """
        data = self.stream.read(amount)

        self.at_eof = not bool(data)
        return data
