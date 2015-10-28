from botornado.s3.bucket import AsyncBucket
from botornado.s3.key import AsyncKey
from tornado.concurrent import return_future
from thumpora.connection import get_asyncS3connection


@return_future
def load(context, url, callback):
    bucket_name, object_name = url.split("/", 1)
    connection = get_asyncS3connection(context)
    bucket = AsyncBucket(connection, bucket_name)
    key = AsyncKey(bucket, object_name)
    key.read(callback=callback)