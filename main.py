import streamlit as st
from configs import UPLOAD_DIR, logging
from data_store import store_file_metadata,get_file_metadata
from df_valdations import validate_file_size, load_and_clean_data
import os
import threading
from file_storage import upload_file
from threads import background_tasks

os.makedirs(UPLOAD_DIR, exist_ok=True)
st.title("CSV File Uploader")

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

# Add a submit button
if uploaded_file:
    if st.button("Submit"):
        progress = st.progress(0, text="Initializing...")

        try:
            logging.info(f"Received file: {uploaded_file.name}")
            progress.progress(10, text="Validating file size...")

            if not validate_file_size(uploaded_file):
                logging.warning("File size validation failed.")
                st.error("❌ File size is too large.")
                st.stop()

            progress.progress(30, text="Reading and cleaning data...")
            df = load_and_clean_data(uploaded_file)
            logging.info(f"File {uploaded_file.name} loaded and cleaned. Rows: {df.shape[0]}")

            progress.progress(50, text="Uploading file...")
            file_path = upload_file(uploaded_file)
            logging.info(f"File uploaded to path: {file_path}")

            progress.progress(70, text="Storing file metadata...")
            file_id = store_file_metadata(
                original_name=uploaded_file.name,
                cloud_path=uploaded_file.name,
                count=df.shape[0]
            )
            logging.info(f"File metadata stored with ID: {file_id}")

            progress.progress(90, text="Starting background tasks...")
            threading.Thread(target=background_tasks, args=(file_path, file_id, df), daemon=True).start()
            logging.info(f"Background thread started for file ID: {file_id}")

            progress.progress(100, text="Completed!")
            st.success("✅ File uploaded and validated successfully!")

        except Exception as e:
            logging.exception("Error occurred while processing the uploaded file")
            st.error(f"❌ Error reading the file: {e}")


st.header("CSV File Upload History")
df = get_file_metadata()
if not df.empty:
    columns_to_display = ["original_name", "status", "created_on", "count"]
    df = df[columns_to_display].reset_index(drop=True)
    st.dataframe(df)


