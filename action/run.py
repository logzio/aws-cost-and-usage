import os
import boto3

from botocore.exceptions import NoCredentialsError

LAMBDA_FUNCTION_CODE_ZIP_FILE_PATH = "{}/lambda_function_code.zip".format(os.environ['GITHUB_WORKSPACE'])
AUTO_DEPLOYMENT_YAML_FILE_PATH = "{}/auto-deployment.yaml".format(os.environ['GITHUB_WORKSPACE'])
LAMBDA_FUNCTION_CODE_ZIP_FILE_NAME = 'lambda_function_code.zip'
AUTO_DEPLOYMENT_YAML_FILE_NAME = 'auto-deployment.yaml'


def empty_s3_bucket(bucket):
    s3resource = boto3.resource('s3')
    bucket = s3resource.Bucket(bucket)

    bucket.objects.delete()
    print("S3 Bucket Is Now Empty")


def upload_to_aws(local_file, bucket, s3_file):
    s3client = boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )

    try:
        s3client.upload_file(local_file, bucket, s3_file, ExtraArgs={'ACL': 'public-read'})
        print("{} was uploaded successfully".format(s3_file))
    except FileNotFoundError:
        print("The file {} was not found".format(local_file))
    except NoCredentialsError:
        print("Credentials not available")


empty_s3_bucket(os.environ['AWS_S3_BUCKET'])
upload_to_aws(LAMBDA_FUNCTION_CODE_ZIP_FILE_PATH, os.environ['AWS_S3_BUCKET'], LAMBDA_FUNCTION_CODE_ZIP_FILE_NAME)
upload_to_aws(AUTO_DEPLOYMENT_YAML_FILE_PATH, os.environ['AWS_S3_BUCKET'], AUTO_DEPLOYMENT_YAML_FILE_NAME)
