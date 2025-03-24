from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
import dask.dataframe as dd
from dask.distributed import Client
import os
import logging
import re
import traceback
from flask_cors import CORS
import subprocess
import pandas as pd
import numpy as np
import uuid
from google.cloud import storage

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = '/mnt/ssd/uploads'
PARQUET_FOLDER = '/mnt/ssd/parquet_files'
ALLOWED_EXTENSIONS = {'csv'}
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
GCP_BUCKET_PREFIX = 'user-data-' #prefix for buckets
GCP_PROJECT_ID = 'readcsv-450710' # project ID
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PARQUET_FOLDER'] = PARQUET_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE
app.secret_key = 'prmis'

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PARQUET_FOLDER, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def is_valid_csv(file_path):
    try:
        dd.read_csv(file_path)
        return True
    except Exception as e:
        logger.error(f"Invalid CSV format: {e}")
        return False

pattern = re.compile(r'(\.exe|\.bat|\.sh|\.cmd|\.msi|\.vbs|\.js|\.ps1)', re.IGNORECASE)

def scan_for_executables_in_chunk(chunk):
    for col in chunk.columns:
        for value in chunk[col].dropna().astype(str):
            cleaned_value = value.strip().replace('"', '').replace("'", "")
            if pattern.search(cleaned_value):
                logger.warning(f"Executable reference detected in chunk for column '{col}': {value}")
                return True
    return False

def create_gcs_bucket(bucket_name):
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.create_bucket(bucket_name)
    logger.info(f"Bucket {bucket.name} created.")
    return bucket.name

@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        logger.error("No file part in the request")
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files["file"]

    if file.filename == "":
        logger.error("No file selected")
        return jsonify({"error": "No file selected"}), 400

    username = request.form.get('username') #get username from form data
    if not username:
        logger.error("Username is required")
        return jsonify({"error": "Username is required"}), 400

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        parquet_path = os.path.join(app.config["PARQUET_FOLDER"], os.path.splitext(filename)[0] + ".parquet")

        try:
            file.save(file_path)
            logger.info(f"File uploaded: {filename}")

            if not is_valid_csv(file_path):
                os.remove(file_path)
                return jsonify({"error": "File rejected: Invalid CSV format!"}), 400

            chunk_size = 500000
            column_data_types = {}

            for chunk in pd.read_csv(file_path, chunksize=chunk_size, dtype=str):
                if scan_for_executables_in_chunk(chunk):
                    os.remove(file_path)
                    return jsonify({"error": "File rejected: Executable references detected!"}), 400

                chunk_data_types = chunk.dtypes
                for column, dtype in chunk_data_types.items():
                    if column not in column_data_types:
                        column_data_types[column] = str(dtype)

            sample_df = pd.read_csv(file_path, nrows=10)
            for col in sample_df.columns:
                if pd.api.types.is_integer_dtype(sample_df[col]):
                    sample_df[col] = pd.to_numeric(sample_df[col], errors='coerce').astype('Int64')

            dtype_dict = {col: str(dtype) for col, dtype in sample_df.dtypes.items()}
            df = dd.read_csv(file_path, blocksize="10MB", dtype=dtype_dict)

            df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
            os.remove(file_path)

            bucket_id = f"{GCP_BUCKET_PREFIX}{uuid.uuid4()}" #create unique bucket ID
            create_gcs_bucket(bucket_id)

            return jsonify({
                "message": "File uploaded and processed successfully (converted to Parquet)",
                "filename": filename,
                "columns": [{"name": col, "data_type": column_data_types[col]} for col in column_data_types.keys()],
                "bucket_id": bucket_id
            }), 200

        except Exception as e:
            trace = traceback.format_exc()
            logger.error(f"Error processing file {filename}: {e}\n{trace}")
            return jsonify({"error": "Internal server error"}), 500

    logger.error("Invalid file type uploaded")
    return jsonify({"error": "Only CSV files are allowed!"}), 400

@app.route("/files", methods=["GET"])
def list_files():
    try:
        files = [f for f in os.listdir(app.config["PARQUET_FOLDER"]) if f.endswith(".parquet")]
        logger.info("Listing uploaded files")
        return jsonify({"files": files}), 200
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        subprocess.run(['git', 'pull', 'origin', 'main'])
        return 'Pulled latest code!', 200
    return 'Invalid request', 400

if __name__ == "__main__":
    try:
        client = Client(n_workers=8, threads_per_worker=2, memory_limit='4GB', host='0.0.0.0')
        dashboard_link = client.dashboard_link
        print(f"Dask dashboard link: {dashboard_link}")
        logger.info(f"Dask dashboard link: {dashboard_link}")
    except Exception as e:
        print(f"Dask client initialization error: {e}")
        logger.error(f"Dask client initialization error: {e}")
        client = None

    if client:
        logger.info("Starting Flask application with Dask")
        app.run(host="0.0.0.0", port=5000, debug=False)
        client.close()