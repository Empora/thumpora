import os
import hashlib

from botornado.s3.bucket import AsyncBucket
from botornado.s3.key import AsyncKey
from boto.s3.key import Key
from dateutil.parser import parse as parse_ts
from thumbor.result_storages import BaseStorage
from boto.s3.bucket import Bucket
from tornado.concurrent import return_future

from thumpora.connection import getS3connection, get_asyncS3connection

s3_connection = None
async_s3_connection = None

class Storage(BaseStorage):

    @property
    def is_auto_webp(self):
        return self.context.config.AUTO_WEBP and self.context.request.accepts_webp

    # Helper method

    def __get_s3_bucket(self):
        return Bucket(
            connection= getS3connection(self.context),
            name=self.context.config.RESULT_STORAGE_BUCKET
        )

    def __get_async_s3_bucket(self):
        return AsyncBucket(
            connection= get_asyncS3connection(self.context),
            name=self.context.config.RESULT_STORAGE_BUCKET
        )

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

    def get_async_key(self):
        file_abspath = self.normalize_path(self.context.request.url)
        file_key=AsyncKey(self.async_storage)
        file_key.key = file_abspath
        return file_key

    def __init__(self, context):
        BaseStorage.__init__(self, context)
        self.storage = self.__get_s3_bucket()
        self.async_storage = self.__get_async_s3_bucket()

    # Interface
    def put(self, bytes):
        file_key = self.get_key()
        file_key.set_contents_from_string(bytes,
                                          encrypt_key = self.context.config.get('S3_STORAGE_SSE', default=False),
                                          reduced_redundancy = self.context.config.get('S3_STORAGE_RRS', default=False)
                                          )

    @return_future
    def get(self, callback):
        key = self.get_async_key()
        key.read(callback=callback)


    @return_future
    def exists(self, path, callback):
        key = self.get_async_key()
        key.exists(callback = callback)

    def remove(self, path):
        key = self.get_key()
        key.delete()

    def last_updated(self):
        path = self.context.request.url
        file_abspath = self.normalize_path(path)
        file_key = self.storage.get_key(file_abspath)

        if not file_key or self.is_expired(file_key):
            return None

        return self.utc_to_local(parse_ts(file_key.last_modified))

