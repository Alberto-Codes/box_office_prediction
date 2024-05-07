import hashlib
import os

from config import Config
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from google.cloud import bigquery, storage
import httpx

app = FastAPI()

# List of IMDb dataset file names to download
dataset_files = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
]


@app.post("/download-data/")
async def download_data():
    """
    Downloads IMDb dataset files and uploads them to GCS.

    This endpoint triggers the download of IMDb dataset files from a URL and
    uploads them to a specific GCS bucket organized by a hash of the project ID.

    Raises:
        HTTPException: If GCP Project ID is missing or an error occurs in data
        transfer processes.

    Returns:
        dict: Confirmation of successful data transfer.
    """
    load_dotenv()  # Load environment variables from the .env file.

    project_id = Config.GOOGLE_CLOUD_PROJECT
    if not project_id:
        raise HTTPException(status_code=500, detail="GCP Project ID is not configured.")

    storage_client = storage.Client(project=project_id)
    project_hash = hashlib.blake2b(project_id.encode(), digest_size=8).hexdigest()
    bucket_name = f"{project_hash}-imdb-datasets"
    bucket = storage_client.bucket(bucket_name)

    async with httpx.AsyncClient() as client:
        for file_name in dataset_files:
            dataset_url = f"https://datasets.imdbws.com/{file_name}"
            try:
                response = await client.get(dataset_url)
                response.raise_for_status()
                data_content = response.content
            except httpx.RequestError as exc:
                raise HTTPException(
                    status_code=500, detail=f"Error downloading data: {str(exc)}"
                )

            blob = bucket.blob(f"raw-datasets/{file_name}")
            blob.upload_from_string(data_content, content_type="application/gzip")

    return {"message": "Data downloaded and uploaded successfully to GCS"}


def load_file_to_bigquery(project_id, bigquery_client, dataset_id, file_name, bucket):
    """
    Loads a file from GCS to a BigQuery table.

    Args:
        project_id (str): Google Cloud Project ID.
        bigquery_client (bigquery.Client): Client for BigQuery service.
        dataset_id (str): ID of the BigQuery dataset.
        file_name (str): Name of the file to load.
        bucket (storage.Bucket): GCS bucket containing the file.

    Outputs:
        Logs the completion of data loading to BigQuery.
    """
    job_config = bigquery.LoadJobConfig(
        field_delimiter="\t",
        skip_leading_rows=1,
        encoding="UTF-8",
        null_marker=r"\N",
        quote_character="",
    )
    uri = f"gs://{bucket.name}/{file_name}"
    table_name = file_name.replace(".tsv.gz", "").replace(".", "_").replace("raw-datasets/", "")
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete


@app.post("/load-data-to-bigquery/")
async def load_data_to_bigquery():
    """
    Loads downloaded data from GCS to BigQuery.

    This endpoint facilitates the loading of IMDb dataset files from GCS into a
    specified BigQuery dataset.

    Raises:
        HTTPException: If the GCP Project ID is missing.

    Returns:
        dict: Confirmation of successful data loading to BigQuery.
    """
    load_dotenv()

    project_id = Config.GOOGLE_CLOUD_PROJECT
    dataset_id = "imdb_dataset"

    if not project_id:
        raise HTTPException(status_code=500, detail="GCP Project ID is not configured.")

    storage_client = storage.Client(project=project_id)
    bigquery_client = bigquery.Client(project=project_id)
    project_hash = hashlib.blake2b(project_id.encode(), digest_size=8).hexdigest()
    bucket_name = f"{project_hash}-imdb-datasets"
    bucket = storage_client.bucket(bucket_name)

    for file_name in dataset_files:
        file_name = f"raw-datasets/{file_name}"
        load_file_to_bigquery(
            project_id, bigquery_client, dataset_id, file_name, bucket
        )

    return {"message": "Data loaded successfully to BigQuery"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
