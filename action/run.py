import os
import boto3
import logging

from botocore.exceptions import NoCredentialsError

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

LAMBDA_FUNCTION_CODE_ZIP_FILE_NAME = 'lambda_function_code.zip'
LAMBDA_FUNCTION_CODE_ZIP_FILE_LOCAL_PATH = "{0}/{1}".format(os.environ['SOURCE_DIR'], LAMBDA_FUNCTION_CODE_ZIP_FILE_NAME)
LAMBDA_FUNCTION_CODE_ZIP_FILE_S3_PATH = "{0}/{1}".format(os.environ['FOLDER_NAME'], LAMBDA_FUNCTION_CODE_ZIP_FILE_NAME)
AUTO_DEPLOYMENT_YAML_FILE_NAME = 'auto-deployment.yaml'
AUTO_DEPLOYMENT_YAML_FILE_LOCAL_PATH = "{0}/{1}".format(os.environ['SOURCE_DIR'], AUTO_DEPLOYMENT_YAML_FILE_NAME)
AUTO_DEPLOYMENT_YAML_FILE_S3_PATH = "{0}/{1}".format(os.environ['FOLDER_NAME'], AUTO_DEPLOYMENT_YAML_FILE_NAME)


def upload_to_aws(local_file, bucket, s3_file):
    s3client = boto3.client('s3')
    
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
    upload_to_aws(LAMBDA_FUNCTION_CODE_ZIP_FILE_LOCAL_PATH, os.environ['AWS_S3_BUCKET'], LAMBDA_FUNCTION_CODE_ZIP_FILE_S3_PATH)
    upload_to_aws(AUTO_DEPLOYMENT_YAML_FILE_LOCAL_PATH, os.environ['AWS_S3_BUCKET'], AUTO_DEPLOYMENT_YAML_FILE_S3_PATH)


if __name__ == '__main__':
    main()
