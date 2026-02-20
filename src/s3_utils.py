import boto3
import os

S3_BUCKET = os.environ.get("S3_BUCKET", "trade-surveillance-data-au")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

def get_s3_client():
    return boto3.client("s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=AWS_REGION
        )

def upload_to_s3(local_path, s3_key):
    s3 = get_s3_client()
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"Uploaded {s3_key} to S3")

def download_from_s3(s3_key, local_path):
    s3 = get_s3_client()
    s3.download_file(S3_BUCKET, s3_key, local_path)
    print(f"Downloaded {s3_key} from S3")

def file_exists_in_s3(s3_key):
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except s3.exceptions.ClientError:
        return False
    
if __name__ == "__main__":
    # Test the connection
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"AWS Region: {AWS_REGION}")
    
    # Test upload (create a test file first)
    test_file = './data/test.txt'
    os.makedirs('./data', exist_ok=True)
    with open(test_file, 'w') as f:
        f.write('Hello S3!')
    
    upload_to_s3(test_file, 'test/test.txt')
    print("Upload test passed!")
    
    # Test download
    download_from_s3('test/test.txt', './data/test_downloaded.txt')
    print("Download test passed!")
    
    # Cleanup
    os.remove(test_file)
    os.remove('./data/test_downloaded.txt')
    print("Tests complete!")