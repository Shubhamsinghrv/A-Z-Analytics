from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
import logging
import re
import traceback
from flask_cors import CORS
import pandas as pd
from google.cloud import storage
import tempfile
import os

app = Flask(__name__)
CORS(app)

ALLOWED_EXTENSIONS = {'csv'}
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
GCP_BUCKET_NAME = 'intelli-ana-bucket'
GCP_PROJECT_ID = 'decisive-sylph-449809-j4'

app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE
app.secret_key = 'prmis'

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
        pd.read_csv(file_path, nrows=5)
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
                logger.warning(f"Executable reference detected in column '{col}': {value}")
                return True
    return False

def upload_to_gcs(bucket_name, file_path, destination_blob_name):
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    logger.info(f"File {file_path} uploaded to {bucket_name}/{destination_blob_name}.")

@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        logger.error("No file part in the request")
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files["file"]
    if file.filename == "":
        logger.error("No file selected")
        return jsonify({"error": "No file selected"}), 400

    username = request.form.get('username')
    if not username:
        logger.error("Username is required")
        return jsonify({"error": "Username is required"}), 400

    if file and allowed_file(file.filename):
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                file.save(temp_file.name)
                file_path = temp_file.name
                logger.info(f"File saved to temporary location: {file_path}")

                if not is_valid_csv(file_path):
                    os.remove(file_path)
                    return jsonify({"error": "File rejected: Invalid CSV format!"}), 400

                chunk_size = 500000
                for chunk in pd.read_csv(file_path, chunksize=chunk_size, dtype=str):
                    if scan_for_executables_in_chunk(chunk):
                        os.remove(file_path)
                        return jsonify({"error": "File rejected: Executable references detected!"}), 400

                # Rename the file to df.csv before uploading
                destination_filename = "df.csv"

                upload_to_gcs(GCP_BUCKET_NAME, file_path, destination_filename)
                os.remove(file_path)

                return jsonify({
                    "message": "File uploaded successfully to GCP",
                    "filename": destination_filename,
                    "bucket_name": GCP_BUCKET_NAME
                }), 200

        except Exception as e:
            trace = traceback.format_exc()
            logger.error(f"Error processing file {file.filename}: {e}\n{trace}")
            return jsonify({"error": "Internal server error"}), 500

    logger.error("Invalid file type uploaded")
    return jsonify({"error": "Only CSV files are allowed!"}), 400

@app.route("/files", methods=["GET"])
def list_files():
    try:
        storage_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = storage_client.bucket(GCP_BUCKET_NAME)
        blobs = bucket.list_blobs()
        files = [blob.name for blob in blobs if blob.name.endswith(".csv")]
        logger.info("Listing uploaded files from GCS")
        return jsonify({"files": files}), 200
    except Exception as e:
        logger.error(f"Error listing files from GCS: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        return 'Webhook received!', 200
    return 'Invalid request', 400

if __name__ == "__main__":
    logger.info("Starting Flask application")
    app.run(host="0.0.0.0", port=5000, debug=False)