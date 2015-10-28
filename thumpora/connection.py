import logging
from botornado.s3.connection import AsyncS3Connection
from boto.s3.connection import S3Connection

s3_connection = None
async_s3_connection = None

def get_asyncS3connection(context):
    conn = async_s3_connection
    if conn is None:
        if context.config.get('AWS_ROLE_BASED_CONNECTION', default=False):
            conn = AsyncS3Connection("KEY", "SECRET")
            logging.info("connection without config")
        else:
            conn = AsyncS3Connection(
                context.config.get('AWS_ACCESS_KEY'),
                context.config.get('AWS_SECRET_KEY')
            )
            logging.info("connection with config: " + context.config.get('AWS_ACCESS_KEY'))
    return conn

def getS3connection(context):
    conn = s3_connection
    if conn is None:
        if context.config.get('AWS_ROLE_BASED_CONNECTION', default=False):
            conn = S3Connection()
            logging.info("connection without config")
        else:
            conn = S3Connection(
                context.config.get('AWS_ACCESS_KEY'),
                context.config.get('AWS_SECRET_KEY')
            )
            logging.info("connection with config: " + context.config.get('AWS_ACCESS_KEY'))
    return conn