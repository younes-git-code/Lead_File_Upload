import pandas as pd

from configs import datastore_client,logging
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud import datastore

def store_file_metadata(original_name: str, cloud_path: str, count: int) -> str:
    file_id = str(uuid.uuid4())
    now = datetime.now(ZoneInfo("America/New_York")).isoformat()
    key = datastore_client.key("LeadFile", file_id)
    entity = datastore.Entity(key=key)
    entity.update({
        "id": file_id,
        "original_name": original_name,
        "cloud_path": cloud_path,
        "created_on": now,
        "updated_on": now,
        "count": count,
        "duplicates": 0,
        "status": "pending"
    })
    datastore_client.put(entity)

    logging.info(f"Stored file metadata for {original_name} with ID {file_id} and status 'pending'")
    return file_id

def get_file_metadata() -> pd.DataFrame:
    query = datastore_client.query(kind="LeadFile")
    results = list(query.fetch())
    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(by="created_on", ascending=False).reset_index(drop=True)
    return df


def update_status_file_metadata(file_id: str, status: str,duplicates : int) -> None:
    key = datastore_client.key("LeadFile", file_id)
    entity = datastore_client.get(key)
    if entity is None:
        logging.error(f"Failed to update status: No entity found with ID {file_id}")
        raise ValueError(f"No entity found with id: {file_id}")

    entity["status"] = status
    entity["updated_on"] = datetime.utcnow().isoformat()
    entity["duplicates"] = duplicates
    datastore_client.put(entity)

    logging.info(f"Updated file ID {file_id} status to '{status}'")


def insert_lead_data(file_id, df):
    now = datetime.now(ZoneInfo("America/New_York")).isoformat()
    with datastore_client.transaction():
        for idx, row in df.iterrows():

            logging.info(f"Row number {idx} ph : {row.phone_number} ")
            lead_id = str(uuid.uuid4())
            key = datastore_client.key("LeadData", lead_id)

            entity = datastore.Entity(key=key)
            entity.update({
                "id": lead_id,
                "fileId": file_id,
                "phone_code": row.phone_code,
                "phone_number": row.phone_number,
                "state": row.state,
                "carrier_type": row.carrier_type,
                "first_name": row.first_name,
                "last_name": row.last_name,
                "postal_code": row.postal_code,
                "address1": row.address1,
                "city": row.city,
                "date_of_birth": row.date_of_birth,
                "gender": row.gender,
                "auth_token": row.auth_token,
                "created_on": now,
                "updated_on": now,
                "error": "",
                "status": "new"
            })

            datastore_client.put(entity)

