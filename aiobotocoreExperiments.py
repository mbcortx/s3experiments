
import asyncio
from aiobotocore.session import get_session


AWS_ACCESS_KEY_ID = "xxx"
AWS_SECRET_ACCESS_KEY = "xxx"


async def uploadObject(bucketName, objectName):

    session = get_session()
    async with session.create_client(
            service_name='s3', 
            endpoint_url="http://127.0.0.1:9002",
            aws_secret_access_key="serversecretkey",
            aws_access_key_id="serveraccesskey") as client:
        data = b'\x01' * 1024
        print(bucketName,objectName)
        resp = await client.put_object(Bucket=bucketName,
                                       Key=objectName,
                                       Body=data)
        print(resp)

        resp = await client.head_object(Bucket=bucketName, Key=objectName)
        print(resp)
   

if __name__ == '__main__':
    asyncio.run(uploadObject("bucket1", "aiotest"))

