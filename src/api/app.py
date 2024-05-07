import gzip
import hashlib
import io
import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List

import httpx
from config import Config
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from google.cloud import bigquery, storage

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
    Download multiple IMDb dataset files and upload them to Google Cloud Storage.

    This endpoint initiates the downloading of dataset files from IMDb and uploads
    them to a specified Google Cloud Storage bucket, organizing them within a
    directory based on the project ID hash.

    Raises:
        HTTPException: If the Google Cloud Project ID is not configured or if there
        is an error in downloading or uploading the data.

    Returns:
        dict: Confirmation message indicating successful data transfer.
    """
    load_dotenv()  # Load environment variables from the .env file.

    # Get GCP project ID from config
    project_id = Config.GOOGLE_CLOUD_PROJECT
    if not project_id:
        raise HTTPException(status_code=500, detail="GCP Project ID is not configured.")

    # Initialize Google Cloud Storage client
    storage_client = storage.Client(project=project_id)
    project_hash = hashlib.blake2b(project_id.encode(), digest_size=8).hexdigest()
    bucket_name = f"{project_hash}-imdb-datasets"
    bucket = storage_client.bucket(bucket_name)

    async with httpx.AsyncClient() as client:
        for file_name in dataset_files:
            # Construct the URL of the IMDb dataset to download
            dataset_url = f"https://datasets.imdbws.com/{file_name}"

            try:
                # Download dataset
                response = await client.get(dataset_url)
                response.raise_for_status()
                data_content = response.content
            except httpx.RequestError as exc:
                raise HTTPException(
                    status_code=500, detail=f"Error downloading data: {str(exc)}"
                )

            # Upload the downloaded data to Google Cloud Storage
            blob = bucket.blob(f"raw-datasets/{file_name}")
            blob.upload_from_string(data_content, content_type="application/gzip")

    return {"message": "Data downloaded and uploaded successfully to GCS"}


# # Define the schema for each IMDb file
# schema_definitions = {
#     "name.basics": [
#         bigquery.SchemaField("nconst", "STRING"),
#         bigquery.SchemaField("primaryName", "STRING"),
#         bigquery.SchemaField("birthYear", "INTEGER"),
#         bigquery.SchemaField("deathYear", "INTEGER"),
#         bigquery.SchemaField("primaryProfession", "STRING", mode="REPEATED"),
#         bigquery.SchemaField("knownForTitles", "STRING", mode="REPEATED"),
#     ],
#     "title.akas": [
#         bigquery.SchemaField("titleId", "STRING"),
#         bigquery.SchemaField("ordering", "INTEGER"),
#         bigquery.SchemaField("title", "STRING"),
#         bigquery.SchemaField("region", "STRING"),
#         bigquery.SchemaField("language", "STRING"),
#         bigquery.SchemaField("types", "STRING", mode="REPEATED"),
#         bigquery.SchemaField("attributes", "STRING", mode="REPEATED"),
#         bigquery.SchemaField("isOriginalTitle", "BOOLEAN"),
#     ],
#     "title.basics": [
#         bigquery.SchemaField("tconst", "STRING"),
#         bigquery.SchemaField("titleType", "STRING"),
#         bigquery.SchemaField("primaryTitle", "STRING"),
#         bigquery.SchemaField("originalTitle", "STRING"),
#         bigquery.SchemaField("isAdult", "BOOLEAN"),
#         bigquery.SchemaField("startYear", "INTEGER"),
#         bigquery.SchemaField("endYear", "INTEGER"),
#         bigquery.SchemaField("runtimeMinutes", "INTEGER"),
#         bigquery.SchemaField("genres", "STRING", mode="REPEATED"),
#     ],
#     "title.crew": [
#         bigquery.SchemaField("tconst", "STRING"),
#         bigquery.SchemaField("directors", "STRING", mode="REPEATED"),
#         bigquery.SchemaField("writers", "STRING", mode="REPEATED"),
#     ],
#     "title.episode": [
#         bigquery.SchemaField("tconst", "STRING"),
#         bigquery.SchemaField("parentTconst", "STRING"),
#         bigquery.SchemaField("seasonNumber", "INTEGER"),
#         bigquery.SchemaField("episodeNumber", "INTEGER"),
#     ],
#     "title.principals": [
#         bigquery.SchemaField("tconst", "STRING"),
#         bigquery.SchemaField("ordering", "INTEGER"),
#         bigquery.SchemaField("nconst", "STRING"),
#         bigquery.SchemaField("category", "STRING"),
#         bigquery.SchemaField("job", "STRING"),
#         bigquery.SchemaField("characters", "STRING"),
#     ],
#     "title.ratings": [
#         bigquery.SchemaField("tconst", "STRING"),
#         bigquery.SchemaField("averageRating", "FLOAT"),
#         bigquery.SchemaField("numVotes", "INTEGER"),
#     ],
# }


def load_file_to_bigquery(project_id, bigquery_client, dataset_id, file_name, bucket):
    job_config = bigquery.LoadJobConfig(
        # autodetect=True,
        field_delimiter="\t",
        skip_leading_rows=1,
        encoding="UTF-8",
        null_marker=r"\N",
        quote_character="",
    )
    uri = f"gs://{bucket.name}/{file_name}"
    table_name = (
        file_name.replace(".tsv.gz", "").replace(".", "_").replace("raw-datasets/", "")
    )
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    # Load the data into BigQuery
    load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete


@app.post("/load-data-to-bigquery/")
async def load_data_to_bigquery():
    load_dotenv()  # Load environment variables from the .env file.

    # Get GCP project ID and BigQuery dataset ID from config
    project_id = Config.GOOGLE_CLOUD_PROJECT
    dataset_id = "imdb_dataset"  # Customize as needed

    if not project_id:
        raise HTTPException(status_code=500, detail="GCP Project ID is not configured.")

    # Initialize Google Cloud Storage and BigQuery clients
    storage_client = storage.Client(project=project_id)
    bigquery_client = bigquery.Client(project=project_id)

    project_hash = hashlib.blake2b(project_id.encode(), digest_size=8).hexdigest()
    bucket_name = f"{project_hash}-imdb-datasets"
    bucket = storage_client.bucket(bucket_name)

    for file_name in dataset_files:
        file_name = f"raw-datasets/{file_name}"  # Add the subdirectory to the file name
        load_file_to_bigquery(
            project_id, bigquery_client, dataset_id, file_name, bucket
        )

    return {"message": "Data loaded successfully to BigQuery"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
