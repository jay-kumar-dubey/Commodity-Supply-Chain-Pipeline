# Central config — all settings loaded from environment variables
import os
from dotenv import load_dotenv

load_dotenv()

EIA_API_KEY = os.environ.get("EIA_API_KEY")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME")
