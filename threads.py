import os
from data_store import insert_lead_data, update_status_file_metadata
from configs import logging


def background_tasks(file_path, file_id, df):
    try:
        logging.info(f"Starting lead insertion for file ID: {file_id}")
        insert_lead_data(file_id, df)
        update_status_file_metadata(file_id, "success",0)
        logging.info(f"Updated file status to 'success' for file ID: {file_id}")
        os.remove(file_path)
        logging.info(f"Deleted file at path: {file_path}")

    except Exception as e:
        update_status_file_metadata(file_id, "failed",0)
        logging.error(f"Error in background thread for file ID {file_id}: {e}", exc_info=True)
