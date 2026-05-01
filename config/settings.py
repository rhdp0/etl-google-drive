import os
from dotenv import load_dotenv

load_dotenv()

LANDING_FOLDER_ID = os.getenv('LANDING_FOLDER_ID')
RAW_FOLDER_ID = os.getenv('RAW_FOLDER_ID')
ARCHIVE_FOLDER_ID = os.getenv('ARCHIVE_FOLDER_ID')

TRUSTED_SHEET_ID = os.getenv('TRUSTED_SHEET_ID')
