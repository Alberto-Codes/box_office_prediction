import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
