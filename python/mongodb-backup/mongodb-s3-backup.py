import os
import shutil
import subprocess
import boto3
import datetime
import tempfile
import time
from datetime import datetime, timedelta, timezone

date=time.strftime('%l-%M%p-%b-%d-%Y')

# MongoDB and AWS S3 configuration
MONGO_DB = "service"  # Replace with your database name
S3_BUCKET = "service-backup"  # Replace with your bucket name
S3_PATH = f"service/{date}"  # S3 folder path where you want to store backups

# AWS S3 credentials
AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXX"
AWS_REGION = "ap-south-1"

# MongoDB and AWS S3 configuration

retention=2 #days

# Create an S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

def create_mongo_dump():
    """Create MongoDB dump with authentication and return the dump file path."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dump_path = os.path.join(tempfile.gettempdir(), f"mongo_dump_{timestamp}.tar.gz")
    if os.path.exists("/home/ubuntu/dump"):
        print("Removing existing dump folder")
        os.remove(dump_file)


    # Run mongodump command with authentication and compress the output
    try:
        subprocess.check_call([
            "mongodump", "--db=service"
        ])
        subprocess.check_call(["tar","-zcvf",dump_path,"dump"])
        print(f"MongoDB dump created at {dump_path}")
        return dump_path
    except subprocess.CalledProcessError as e:
        print(f"Error creating MongoDB dump: {e}")
        return None

def upload_to_s3(file_path):
    """Upload a file to S3."""
    s3_file_name = os.path.join(S3_PATH, os.path.basename(file_path))
    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_file_name)
        print(f"File uploaded to S3: s3://{S3_BUCKET}/{s3_file_name}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

def get_all_folders(bucket_name, prefix="",retention=5):
    """Get a list of folders in an S3 bucket."""
    folders = []
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='service/')
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention)
    for i in response['Contents']:
        if ( i['LastModified'] < cutoff_date ):
            print(i['Key'])
            folders.append(i['Key'])
    return folders

def delete_folder(bucket_name,list_files, prefix=""):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='service/')
    for i in list_files:
        response = s3_client.delete_object(Bucket=bucket_name, Key=f"{i}")
        print(response)


def main():
    dump_file = create_mongo_dump()
    if dump_file:
        upload_to_s3(dump_file)
        shutil.rmtree("/home/ubuntu/dump")
        os.remove(dump_file)
        list_files=get_all_folders(S3_BUCKET,retention=retention,prefix="")
        if len(list_files) > 0:
            delete_folder(S3_BUCKET,list_files,prefix="")

if __name__ == "__main__":
    main()

