from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.app import app

client = TestClient(app)


def test_download_data():
    # Mock the storage.Client class
    with patch("google.cloud.storage.Client") as MockClient:
        # Mock the instance of the Client class
        mock_client_instance = MockClient.return_value

        # Mock the bucket method
        mock_bucket = MagicMock()
        mock_client_instance.bucket.return_value = mock_bucket

        # Mock the blob method
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        # Mock the upload_from_string method
        mock_blob.upload_from_string.return_value = None

        # Mock the httpx.AsyncClient class
        with patch("httpx.AsyncClient") as MockAsyncClient:
            # Mock the instance of the AsyncClient class
            mock_async_client_instance = MockAsyncClient.return_value

            # Mock the get method
            mock_get = MagicMock()
            mock_async_client_instance.get.return_value = mock_get

            # Mock the content attribute
            mock_get.content = b"test content"

            # Mock the raise_for_status method
            mock_get.raise_for_status.return_value = None

            # Send a POST request to the download_data endpoint
            response = client.post("/download-data/")

    # Assert that the response status code is 200
    assert response.status_code == 200

    # Assert that the response JSON is as expected
    assert response.json() == {
        "message": "Data downloaded and uploaded successfully to GCS"
    }
