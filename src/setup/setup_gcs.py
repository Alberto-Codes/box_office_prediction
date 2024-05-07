import hashlib
import os

from config import Config
from dotenv import load_dotenv
from google.cloud import bigquery, storage


def create_bucket_and_directories(bucket_name_suffix):
    """
    Creates a GCS bucket and directories using a hashed project ID.

    The function uses the hashed value of the GCP project ID to ensure the
    bucket name is unique and reproducible across multiple invocations.

    Args:
        bucket_name_suffix (str): Suffix for the bucket name to ensure uniqueness.

    Raises:
        ValueError: If the GCP project ID is not set in the environment.

    Outputs:
        Logs the status of bucket and directory creation.
    """
    load_dotenv()

    project_id = Config.GOOGLE_CLOUD_PROJECT
    if not project_id:
        raise ValueError("GCP_PROJECT_ID is not set in the environment.")

    project_hash = hashlib.blake2b(project_id.encode(), digest_size=8).hexdigest()
    bucket_name = f"{project_hash}-{bucket_name_suffix}"

    client = storage.Client(project=project_id)

    bucket = client.lookup_bucket(bucket_name)
    if not bucket:
        bucket = client.create_bucket(bucket_name)
        print(f"Bucket {bucket_name} created.")
    else:
        print(f"Bucket {bucket_name} already exists.")

    directories = ["raw-datasets/", "processed-datasets/", "logs/"]
    for directory in directories:
        blob = bucket.blob(directory)
        blob.upload_from_string("")
        print(f"Directory {directory} created in bucket {bucket_name}.")


def create_bigquery_dataset(dataset_id):
    """
    Ensures the existence of a specified BigQuery dataset.

    This function checks and creates a BigQuery dataset if it does not already
    exist, aiding in data organization.

    Args:
        dataset_id (str): Identifier for the dataset to check or create.

    Raises:
        ValueError: If the GCP project ID is not set in the environment.

    Outputs:
        Logs the dataset's status (existence or creation).
    """
    load_dotenv()

    project_id = Config.GOOGLE_CLOUD_PROJECT
    if not project_id:
        raise ValueError("GCP_PROJECT_ID is not set in the environment.")

    client = bigquery.Client(project=project_id)

    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Dataset {dataset_id} created.")


def create_bigquery_tables(dataset_id):
    """
    Creates tables in a specified BigQuery dataset according to IMDb schema.

    Args:
        dataset_id (str): Identifier of the dataset for table creation.

    Raises:
        ValueError: If the GCP project ID is not set in the environment.

    Outputs:
        Logs the status of table creation.
    """
    load_dotenv()

    project_id = Config.GOOGLE_CLOUD_PROJECT
    if not project_id:
        raise ValueError("GCP_PROJECT_ID is not set in the environment.")

    client = bigquery.Client(project=project_id)

    table_schemas = {
        "name_basics": [
            bigquery.SchemaField("nconst", "STRING"),
            bigquery.SchemaField("primaryName", "STRING"),
            bigquery.SchemaField("birthYear", "INTEGER"),
            bigquery.SchemaField("deathYear", "INTEGER"),
            bigquery.SchemaField("primaryProfession", "STRING"),
            bigquery.SchemaField("knownForTitles", "STRING"),
        ],
        "title_akas": [
            bigquery.SchemaField("titleId", "STRING"),
            bigquery.SchemaField("ordering", "INTEGER"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("language", "STRING"),
            bigquery.SchemaField("types", "STRING"),
            bigquery.SchemaField("attributes", "STRING"),
            bigquery.SchemaField("isOriginalTitle", "BOOLEAN"),
        ],
        "title_basics": [
            bigquery.SchemaField("tconst", "STRING"),
            bigquery.SchemaField("titleType", "STRING"),
            bigquery.SchemaField("primaryTitle", "STRING"),
            bigquery.SchemaField("originalTitle", "STRING"),
            bigquery.SchemaField("isAdult", "BOOLEAN"),
            bigquery.SchemaField("startYear", "INTEGER"),
            bigquery.SchemaField("endYear", "INTEGER"),
            bigquery.SchemaField("runtimeMinutes", "INTEGER"),
            bigquery.SchemaField("genres", "STRING"),
        ],
        "title_crew": [
            bigquery.SchemaField("tconst", "STRING"),
            bigquery.SchemaField("directors", "STRING"),
            bigquery.SchemaField("writers", "STRING"),
        ],
        "title_episode": [
            bigquery.SchemaField("tconst", "STRING"),
            bigquery.SchemaField("parentTconst", "STRING"),
            bigquery.SchemaField("seasonNumber", "INTEGER"),
            bigquery.SchemaField("episodeNumber", "INTEGER"),
        ],
        "title_principals": [
            bigquery.SchemaField("tconst", "STRING"),
            bigquery.SchemaField("ordering", "INTEGER"),
            bigquery.SchemaField("nconst", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("job", "STRING"),
            bigquery.SchemaField("characters", "STRING"),
        ],
        "title_ratings": [
            bigquery.SchemaField("tconst", "STRING"),
            bigquery.SchemaField("averageRating", "FLOAT"),
            bigquery.SchemaField("numVotes", "INTEGER"),
        ],
    }

    for table_id, schema in table_schemas.items():
        table_ref = client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table_id} in dataset {dataset_id}")


if __name__ == "__main__":
    bucket_name_suffix = "imdb-datasets"
    create_bucket_and_directories(bucket_name_suffix)

    dataset_id = "imdb_dataset"
    create_bigquery_dataset(dataset_id)
    create_bigquery_tables(dataset_id)
