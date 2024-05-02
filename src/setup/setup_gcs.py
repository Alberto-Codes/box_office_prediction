import hashlib
import os

from config import Config
from dotenv import load_dotenv
from google.cloud import bigquery, storage


def create_bucket_and_directories(bucket_name_suffix):
    """
    Creates a Google Cloud Storage (GCS) bucket and predefined directories within it.

    This function uses the hashed value of the Google Cloud Project ID to ensure the
    bucket name remains unique yet consistently reproducible across multiple invocations.

    Args:
        bucket_name_suffix (str): A suffix to append to the bucket name to ensure uniqueness.

    Raises:
        ValueError: If the GCP project ID is not set in the environment.

    Outputs:
        Logs the status of bucket and directory creation to the console.
    """
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

    # Check if the bucket already exists
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


def create_bigquery_dataset(dataset_id):
    """
    Ensures the existence of a BigQuery dataset.

    The function checks for the existence of a specified dataset and creates it if it
    does not exist, helping maintain data organization within BigQuery.

    Args:
        dataset_id (str): The identifier for the dataset to check or create.

    Raises:
        ValueError: If the GCP project ID is not set in the environment.

    Outputs:
        Logs the status of dataset existence or creation to the console.
    """
    # Load environment variables
    load_dotenv()

    # Get GCP project ID from environment
    project_id = Config.GOOGLE_CLOUD_PROJECT
    if not project_id:
        raise ValueError("GCP_PROJECT_ID is not set in the environment.")

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Check if the dataset already exists
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except:
        # Create the dataset if it doesn't exist
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(
            dataset, timeout=30
        )  # Added timeout parameter for API call
        print(f"Dataset {dataset_id} created.")


if __name__ == "__main__":
    bucket_name_suffix = "imdb-datasets"  # Customize as needed
    create_bucket_and_directories(bucket_name_suffix)

    dataset_id = "imdb_dataset"  # Customize as needed
    create_bigquery_dataset(dataset_id)
