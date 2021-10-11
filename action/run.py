import os
import boto3
import logging

from botocore.exceptions import NoCredentialsError

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

LAMBDA_FUNCTION_CODE_ZIP_FILE_NAME = 'lambda_function_code.zip'
LAMBDA_FUNCTION_CODE_ZIP_FILE_PATH = "{0}/{1}".format(os.environ['SOURCE_DIR'], LAMBDA_FUNCTION_CODE_ZIP_FILE_NAME)
AUTO_DEPLOYMENT_YAML_FILE_NAME = 'auto-deployment.yaml'
AUTO_DEPLOYMENT_YAML_FILE_PATH = "{0}/{1}".format(os.environ['SOURCE_DIR'], AUTO_DEPLOYMENT_YAML_FILE_NAME)

s3client = boto3.client('s3')


def delete_bucket_folder_files(bucket, folder):
    s3resource = boto3.resource('s3')
    bucket = s3resource.Bucket(bucket)

    bucket.objects.filter(Prefix=folder).delete()
    s3client.put_object(bucket, folder)

    print("S3 bucket directory is now empty")
    

def upload_to_aws(local_file, bucket, s3_file):
    try:
        s3client.upload_file(local_file, bucket, s3_file, ExtraArgs={'ACL': 'public-read'})
        print("{} was uploaded successfully".format(s3_file))
    except FileNotFoundError:
        print("The file {} was not found".format(local_file))
        exit(1)
    except NoCredentialsError:
        print("Credentials not available")
        exit(1)


def main():
    delete_bucket_folder_files(os.environ['AWS_S3_BUCKET'], os.environ['FOLDER_NAME'])
    upload_to_aws(LAMBDA_FUNCTION_CODE_ZIP_FILE_PATH, os.environ['AWS_S3_BUCKET'], LAMBDA_FUNCTION_CODE_ZIP_FILE_NAME)
    upload_to_aws(AUTO_DEPLOYMENT_YAML_FILE_PATH, os.environ['AWS_S3_BUCKET'], AUTO_DEPLOYMENT_YAML_FILE_NAME)


if __name__ == '__main__':
    main()
