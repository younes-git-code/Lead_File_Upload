from configs import MAX_FILE_SIZE_MB,MAX_RECORDS,EXPECTED_COLUMNS
import streamlit as st
import pandas as pd

def validate_file_size(file):
    if file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
        st.error(f"❌ The uploaded file exceeds {MAX_FILE_SIZE_MB} MB.")
        return False
    return True

def validate_headers(df):
    uploaded_headers = [col.strip() for col in df.columns]
    if uploaded_headers != EXPECTED_COLUMNS:
        st.error("❌ CSV headers do not match the expected format.")
        st.write("Expected headers:", EXPECTED_COLUMNS)
        st.write("Uploaded headers:", uploaded_headers)
        return False
    return True

def validate_data(df):
    if len(df) > MAX_RECORDS:
        st.error(f"❌ The file has more than {MAX_RECORDS} records.")
        return False
    if df['phone_number'].isnull().any():
        st.error("❌ 'phone_number' column contains null or NaN values.")
        return False
    if not df['phone_number'].is_unique:
        st.error("❌ 'phone_number' column must contain unique values.")
        return False
    return True

def clean_dataframe(df):
    df = df.fillna("null").astype(str)
    df.replace(r'^\s*$', "null", regex=True, inplace=True)
    return df

def load_and_clean_data(uploaded_file):
    df = pd.read_csv(uploaded_file)

    if not validate_headers(df):
        st.stop()
    if not validate_data(df):
        st.stop()

    df = clean_dataframe(df)
    return df
