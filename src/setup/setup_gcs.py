import hashlib
import os

from dotenv import load_dotenv
from google.cloud import storage

from config import Config


def create_bucket_and_directories(bucket_name_suffix):
    """Create a GCS bucket and directories within it, avoiding direct use of the project ID."""
    # Load environment variables
    load_dotenv()

    # Get GCP project ID from environment
    project_id = Config.GOOGLE_CLOUD_PROJECT
    if not project_id:
        raise ValueError("GCP_PROJECT_ID is not set in the environment.")

    # Generate a hash of the project ID to use in the bucket name
    project_hash = hashlib.blake2b(project_id.encode(), digest_size=8).hexdigest()
    bucket_name = f"{project_hash}-{bucket_name_suffix}"

    # Initialize GCS client
    client = storage.Client(project=project_id)

    # Check if the bucket already existsgcloud auth application-default logingcloud auth application-default login
    bucket = client.lookup_bucket(bucket_name)
    if not bucket:
        # Create new bucket
        bucket = client.create_bucket(bucket_name)
        print(f"Bucket {bucket_name} created.")
    else:
        print(f"Bucket {bucket_name} already exists.")

    # Directories to create within the bucket
    directories = ["raw-datasets/", "processed-datasets/", "logs/"]
    for directory in directories:
        blob = bucket.blob(directory)
        blob.upload_from_string("")
        print(f"Directory {directory} created in bucket {bucket_name}.")


if __name__ == "__main__":
    bucket_name_suffix = "imdb-datasets"  # Customize as needed
    create_bucket_and_directories(bucket_name_suffix)
