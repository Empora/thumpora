import os
from botornado.s3.bucket import AsyncBucket
from botornado.s3.connection import AsyncS3Connection
from botornado.s3.key import AsyncKey
from boto.s3.key import Key
from dateutil.parser import parse as parse_ts
from thumbor.result_storages import BaseStorage
from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection
import hashlib

from tornado.concurrent import return_future

s3_connection = None
async_s3_connection = None

class Storage(BaseStorage):

    @property
    def is_auto_webp(self):
        return self.context.config.AUTO_WEBP and self.context.request.accepts_webp

    # Helper method

    def getS3connection(self):
        conn = s3_connection
        if conn is None:
            if self.context.config.get('AWS_ROLE_BASED_CONNECTION', default=False):
                conn = S3Connection()
            else:
                conn = S3Connection(
                    self.context.config.get('AWS_ACCESS_KEY'),
                    self.context.config.get('AWS_SECRET_KEY')
                )
        return conn

    def __get_s3_bucket(self):
        return Bucket(
            connection= self.getS3connection(),
            name=self.context.config.RESULT_STORAGE_BUCKET
        )

    def get_asyncS3connection(self):
        conn = async_s3_connection
        if conn is None:
            if self.context.config.get('AWS_ROLE_BASED_CONNECTION', default=False):
                conn = AsyncS3Connection()
            else:
                conn = AsyncS3Connection(
                    self.context.config.get('AWS_ACCESS_KEY'),
                    self.context.config.get('AWS_SECRET_KEY')
                )
        return conn


    def normalize_path(self, path):
        root_path = self.context.config.get('RESULT_STORAGE_AWS_STORAGE_ROOT_PATH', default='thumbor/result_storage/')
        path_segments = [path]
        if self.is_auto_webp:
            path_segments.append("webp")
        digest = hashlib.sha1(".".join(path_segments).encode('utf-8')).hexdigest()
        return os.path.join(root_path, digest)

    def get_key(self):
        file_abspath = self.normalize_path(self.context.request.url)
        file_key=Key(self.storage)
        file_key.key = file_abspath
        return file_key

    def get_async_key(self, path):
        bucket_name, object_name = path.split("/", 1)
        connection = self.get_asyncS3connection()
        bucket = AsyncBucket(connection, bucket_name)
        return AsyncKey(bucket, object_name)

    def __init__(self, context):
        BaseStorage.__init__(self, context)
        self.storage = self.__get_s3_bucket()

    # Interface
    def put(self, bytes):
        file_key = self.get_key()
        file_key.set_contents_from_string(bytes,
                                          encrypt_key = self.context.config.get('S3_STORAGE_SSE', default=False),
                                          reduced_redundancy = self.context.config.get('S3_STORAGE_RRS', default=False)
                                          )



    def put_crypto(self, path):
        raise NotImplementedError()

    def put_detector_data(self, path, data):
        raise NotImplementedError()

    @return_future
    def get(self, callback):
        file_abspath = self.normalize_path(self.context.request.url)
        key = self.get_async_key(file_abspath)
        key.read(callback=callback)

    @return_future
    def get(self, callback):
        file_abspath = self.normalize_path(self.context.request.url)
        key = self.get_async_key(file_abspath)
        key.read(callback=callback)


    @return_future
    def exists(self, path, callback):
        key = self.get_key(path)
        key.exists(callback = callback)

    def remove(self, path):
        key = self.get_key(path)
        key.delete()

    def last_updated(self):
        path = self.context.request.url
        file_abspath = self.normalize_path(path)
        file_key = self.storage.get_key(file_abspath)

        if not file_key or self.is_expired(file_key):
            return None

        return self.utc_to_local(parse_ts(file_key.last_modified))

