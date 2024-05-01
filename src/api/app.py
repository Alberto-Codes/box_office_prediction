from fastapi import FastAPI, HTTPException
import httpx
from google.cloud import storage
import os
from dotenv import load_dotenv
from .config import Config
import hashlib

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
    load_dotenv()  # Load environment variables
    
    # Get GCP project ID from config
    project_id = Config.GOOGLE_CLOUD_PROJECT
    if not project_id:
        raise HTTPException(status_code=500, detail="GCP Project ID is not configured.")
    
    # Initialize GCS client
    storage_client = storage.Client(project=project_id)
    project_hash = hashlib.blake2b(project_id.encode(), digest_size=8).hexdigest()
    bucket_name = f"{project_hash}-imdb-datasets"
    bucket = storage_client.bucket(bucket_name)
    
    async with httpx.AsyncClient() as client:
        for file_name in dataset_files:
            # URL of the IMDb dataset to download
            dataset_url = f"https://datasets.imdbws.com/{file_name}"
            
            try:
                # Download dataset
                response = await client.get(dataset_url)
                response.raise_for_status()
                data_content = response.content
            except httpx.RequestError as exc:
                raise HTTPException(status_code=500, detail=f"Error downloading data: {str(exc)}")
            
            # Upload the data to GCS
            blob = bucket.blob(f"raw-datasets/{file_name}")
            blob.upload_from_string(data_content, content_type="application/gzip")
    
    return {"message": "Data downloaded and uploaded successfully to GCS"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)