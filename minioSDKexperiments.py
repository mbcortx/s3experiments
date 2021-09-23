from io import BytesIO
from minio import Minio
from minio.commonconfig import CopySource, Tags
from minio.error import InvalidResponseError, S3Error

def uploadFile(client, bucketName, objectName, filePath):
    try:
        if not client.bucket_exists(bucketName):
            res=client.make_bucket(bucketName)
            print(res)
        else:
            print(bucketName, "already exists")

        res=client.fput_object(bucketName, objectName, filePath)
        print(res)

    except S3Error as err:
        print("error uploadFile ", err)


def copyWithMetadata(client, bucketName, objectName,  sourceBucket, sourceObject):
    
    mmetadata = {"test1": "1"}
    mtags = Tags({"tag1":"1", "tag2":"2"})
    try:
        if not client.bucket_exists(bucketName):
            res=client.make_bucket(bucketName)
            print(res)
        res = client.copy_object(bucketName, objectName, 
                                     CopySource(sourceBucket, sourceObject),
                                     metadata=mmetadata, tags=mtags)
        print(res)

        res=client.stat_object(bucketName, objectName)
        print(str(res.metadata), res.etag, res.version_id)

    except InvalidResponseError as err:
        print(err)


def getObjectTest(client, bucketName, objectName):
    try:
        with client.get_object(bucketName, objectName) as response:
            return response.read()

    except InvalidResponseError as err:
        print (err)


def putObjectTest(client, bucketName, objectName, data):
    try:
        if not client.bucket_exists(bucketName):
            client.make_bucket(bucketName)
            
        client.put_object(bucketName, objectName, BytesIO(data), len(data))
    except InvalidResponseError as err:
        print(err)

def runSomeTests():

    client1 = Minio(
        "127.0.0.1:9001",
        access_key="server1accesskey",
        secret_key="server1secretkey",
        secure=False
    )

    client2 = Minio(
        "127.0.0.1:9002",
        access_key="server2accesskey",
        secret_key="server2secretkey",
        secure=False
    )

    uploadFile(client2, "bucket1", "object1", "/data/bin/minio")
    copyWithMetadata(client2,"bucket22","object22", "bucket1", "object1")
    data = getObjectTest(client2, "bucket1", "object1")
    putObjectTest(client2, "bucket3", "object3", data)

# sudo /data/bin/minio server --address :9001 /data/server1/data{1..16} &
# sudo /data/bin/minio server --address :9002 /data/server2/data{1..16} &

if __name__ == "__main__":
    
        runSomeTests()