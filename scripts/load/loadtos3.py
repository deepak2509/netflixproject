import os
import boto3
import yaml


CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config/config.yaml"))
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)


s3_config = config.get("aws", {})
bucket = s3_config.get("bucket")
access_key = s3_config.get("access_key")
secret_key = s3_config.get("secret_key")
region = s3_config.get("region")


def upload_to_s3(file_path, s3_key):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    s3.upload_file(file_path, bucket, s3_key)
    print(f" Uploaded: {file_path} → s3://{bucket}/{s3_key}")


processed_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/processed"))

for file_name in os.listdir(processed_dir):
    if file_name.endswith(".csv"):
        file_path = os.path.join(processed_dir, file_name)
        s3_key = f"cleaned/{file_name}"  
        upload_to_s3(file_path, s3_key)
