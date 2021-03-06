from botornado.s3.connection import AsyncS3Connection, boto

s3_connection = None
async_s3_connection = None

def get_asyncS3connection(context):
    conn = async_s3_connection
    if conn is None:
        if context.config.get('AWS_ROLE_BASED_CONNECTION', default=False):
            conn = AsyncS3Connection("KEY", "SECRET")
        else:
            conn = AsyncS3Connection(
                aws_access_key_id=context.config.get('AWS_ACCESS_KEY'),
                aws_secret_access_key=context.config.get('AWS_SECRET_KEY')
            )
    return conn

def getS3connection(context):
    conn = s3_connection
    if conn is None:
        if context.config.get('AWS_ROLE_BASED_CONNECTION', default=False):
            conn = boto.connect_s3()
        else:
            conn = boto.connect_s3(
                context.config.get('AWS_ACCESS_KEY'),
                context.config.get('AWS_SECRET_KEY')
            )
    return conn