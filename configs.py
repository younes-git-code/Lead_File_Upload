from google.cloud import storage
from google.cloud import datastore
import logging
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "spartan-vine-456818-e9-c9000351f0f2.json"

bucket_name = "lead_csv_file"
datastore_name = 'lead-files'

#BUCKET_NAME = os.getenv("BUCKET_NAME")
#DATASTORE_NAME = os.getenv("DATASTORE_NAME")

storage_client = storage.Client()
datastore_client = datastore.Client(database=datastore_name)
UPLOAD_DIR = "uploads"
MAX_FILE_SIZE_MB = 20
MAX_RECORDS = 25000

EXPECTED_COLUMNS = [
    'phone_code',
    'phone_number',
    'state',
    'carrier_type',
    'first_name',
    'last_name',
    'postal_code',
    'address1',
    'city',
    'date_of_birth',
    'gender',
    'auth_token'
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
