# GCS Bucket and Directory Setup

This repository contains a Python script to automate the creation of a Google Cloud Storage (GCS) bucket and its designated directories. The script is designed to be run within a GitHub Codespace using a devcontainer configuration that includes the Google Cloud CLI.

## Prerequisites

1. Access to a Google Cloud Platform (GCP) project with the necessary permissions to create GCS buckets.
2. GitHub Codespaces enabled for your repository.

## Setup

1. Open the repository in a GitHub Codespace.
   - The devcontainer configuration file (`devcontainer.json`) will automatically set up the environment with the Google Cloud CLI.

2. Authenticate with GCP using the following command:
   ```
   gcloud auth application-default login
   ```
   - Follow the provided link to log in to your GCP account and grant access to the Codespace.

3. Navigate to the `src/setup` directory:
   ```
   cd src/setup
   ```

4. Install the required Python dependencies using pipenv:
   ```
   pipenv install
   ```

5. Make sure you have set the `GOOGLE_CLOUD_PROJECT` environment variable to your GCP project ID in the `.env` file or in your Codespace secrets.

## Running the Script

To create the GCS bucket and directories, run the following command:
```
pipenv run python setup_gcs.py
```

The script will:
1. Load the GCP project ID from the environment.
2. Generate a hash of the project ID to use as part of the bucket name.
3. Check if the bucket already exists. If not, it will create a new bucket.
4. Create the specified directories within the bucket:
   - `raw-datasets/`
   - `processed-datasets/`
   - `logs/`

## Customization

If you want to customize the bucket name suffix or the directories created within the bucket, you can modify the following lines in the `setup_gcs.py` script:

```python
bucket_name_suffix = 'imdb-datasets'  # Customize as needed
```

```python
directories = ['raw-datasets/', 'processed-datasets/', 'logs/']
```

## Notes

- The script uses the `google-cloud-storage` library to interact with GCS. Make sure it is included in your `Pipfile` and installed via `pipenv install`.
- The script assumes you have set up the necessary GCP project and have the required permissions to create buckets and manage GCS.
- The devcontainer configuration includes the Google Cloud CLI, so you don't need to install it separately.
