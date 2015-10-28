from botornado.s3.bucket import AsyncBucket
from botornado.s3.connection import AsyncS3Connection
from botornado.s3.key import AsyncKey
from tornado.concurrent import return_future

class Storage(BaseStorage):
    def __init__(self, context):
        BaseStorage.__init__(self, context)
        self.storage = self.__get_s3_bucket()


    def put(self, path, bytes):
        bucket_name, object_name = path.split("/", 1)
        connection = AsyncS3Connection() # load credentials from environment
        bucket = AsyncBucket(connection, bucket_name)
        key = AsyncKey(bucket, object_name)
        key.send_file()
        raise NotImplementedError()

    def put_crypto(self, path):
        bucket_name, object_name = path.split("/", 1)
        connection = AsyncS3Connection() # load credentials from environment
        bucket = AsyncBucket(connection, bucket_name)
        key = AsyncKey(bucket, object_name)
        key.send_file()
        raise NotImplementedError()

    def put_detector_data(self, path, data):
        raise NotImplementedError()

    @return_future
    def get(self, path, callback):
        bucket_name, object_name = path.split("/", 1)
        connection = AsyncS3Connection() # load credentials from environment
        bucket = AsyncBucket(connection, bucket_name)
        key = AsyncKey(bucket, object_name)
        key.read(callback=callback)
        raise NotImplementedError()

    @return_future
    def exists(self, path, callback):
        bucket_name, object_name = path.split("/", 1)
        connection = AsyncS3Connection() # load credentials from environment
        bucket = AsyncBucket(connection, bucket_name)
        key = AsyncKey(bucket, object_name)
        key.exists(callback = callback)
        raise NotImplementedError()

    def remove(self, path):
        bucket_name, object_name = path.split("/", 1)
        connection = AsyncS3Connection() # load credentials from environment
        bucket = AsyncBucket(connection, bucket_name)
        key = AsyncKey(bucket, object_name)
        key.delete()
        raise NotImplementedError()
