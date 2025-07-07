import os
import logging
from dotenv import load_dotenv
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Load environment variables from .env
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Initialize S3 resource
try:
    aws_region = os.getenv('AWS_REGION_NAME')
    if not aws_region:
        raise ValueError("Missing AWS_REGION_NAME in .env file.")
    s3_resource = boto3.resource('s3', region_name=aws_region)
except Exception as e:
    logging.error(f"Failed to initialize S3 resource: {e}")
    raise


def s3_upload(file_name, s3_folder, bucket_name):
    """
    Uploads a single file to an S3 bucket under a specified folder.

    Args:
        file_name (str): Local file to upload.
        s3_folder (str): Target folder name inside S3 bucket.
        bucket_name (str): Name of the S3 bucket.

    Returns:
        bool: True if upload is successful, False otherwise.
    """
    try:
        s3_path = f"{s3_folder}/{file_name}" if s3_folder else file_name
        s3_bucket = s3_resource.Bucket(name=bucket_name)

        s3_bucket.upload_file(Filename=file_name, Key=s3_path)
        logging.info(f'Successfully uploaded "{file_name}" to "{bucket_name}/{s3_path}"')
        return True

    except FileNotFoundError:
        logging.error(f'Local file "{file_name}" not found.')
    except (ClientError, BotoCoreError) as e:
        logging.error(f'Boto3 error uploading "{file_name}": {e}')
    except Exception as e:
        logging.error(f'Unexpected error uploading "{file_name}": {e}')

    return False


if __name__ == '__main__':
    # Collect values from .env
    file_names = ['department.csv', 'stores.csv', 'fact.csv']
    s3_folder = os.getenv('AWS_S3_FOLDER_NAME')
    bucket_name = os.getenv('AWS_S3_BUCKET_NAME')

    if not bucket_name:
        logging.error("Missing AWS_S3_BUCKET_NAME in .env")
        exit(1)

    # Upload each file
    for filename in file_names:
        success = s3_upload(filename, s3_folder, bucket_name)
        if not success:
            logging.warning(f'Upload failed for "{filename}"')
