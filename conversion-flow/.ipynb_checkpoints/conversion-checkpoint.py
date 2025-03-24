#!/usr/bin/env python
# coding: utf-8
# %%

# %%


import pandas as pd
import json
import gcsfs
import logging
import time
from google.cloud import storage
from concurrent.futures import ProcessPoolExecutor

# ✅ Configure Logging
logging.basicConfig(
    filename="csv_to_parquet.log",  # Log file
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ✅ GCS Paths
csv_path = "gs://intelli-ana-bucket/df.csv"
parquet_path = "gs://intelli-ana-bucket/df.parquet"
json_gcs_path = "gs://intelli-ana-bucket/column_names.json"

# ✅ Initialize GCS clients
try:
    storage_client = storage.Client()
    fs = gcsfs.GCSFileSystem()
    logging.info("✅ Google Cloud authentication successful!")
except Exception as e:
    logging.error(f"❌ Google Cloud authentication failed: {e}")
    raise

# ✅ Function to process each CSV chunk
def process_chunk(chunk):
    """Processes a single chunk of the CSV file."""
    return chunk  # Modify this if data transformation is required

def main():
    start_time = time.time()
    logging.info("🚀 CSV to Parquet conversion started.")

    num_cores = 8  # Utilize all 8 vCPUs
    chunksize = 500000  # Adjust based on memory

    df_list = []
    
    try:
        with ProcessPoolExecutor(max_workers=num_cores) as executor:
            futures = []
            
            # ✅ Read CSV in chunks and process in parallel
            for chunk in pd.read_csv(csv_path, chunksize=chunksize, storage_options={"token": None}):
                futures.append(executor.submit(process_chunk, chunk))
            
            # ✅ Collect processed chunks
            for future in futures:
                df_list.append(future.result())

        # ✅ Merge processed chunks
        final_df = pd.concat(df_list, ignore_index=True)

        # ✅ Save as Parquet
        final_df.to_parquet(
            parquet_path,
            engine="pyarrow",
            index=False,
            storage_options={"token": None}
        )
        logging.info("✅ Parquet file successfully created and saved to GCS.")

        # ✅ Extract column names and store as JSON
        column_names = {"columns": final_df.columns.tolist()}
        json_data = json.dumps(column_names, indent=4)

        # ✅ Upload JSON metadata to GCS
        with fs.open(json_gcs_path, 'w') as f:
            f.write(json_data)

        logging.info(f"✅ Column names JSON successfully uploaded to {json_gcs_path}")

    except Exception as e:
        logging.error(f"❌ An error occurred: {e}")
        raise

    end_time = time.time()
    logging.info(f"⏳ Total execution time: {round(end_time - start_time, 2)} seconds")

if __name__ == "__main__":
    main()

