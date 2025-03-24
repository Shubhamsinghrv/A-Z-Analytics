# Intelligence Analytics

## 📌 Overview
This project automates the data processing workflow using **Google Cloud Platform (GCP)** services. It dynamically creates storage buckets, processes CSV files into Parquet format, normalizes data, generates visualizations, and runs predictions.

## 🚀 Workflow
1. **Bucket Creation (Terraform)**: A new Google Cloud Storage (GCS) bucket is dynamically created when needed.
2. **CSV File Upload**: After validation, CSV files are added to the bucket.
3. **Conversion to Parquet (Cloud Run - `conversion.py`)**:
   - Pub/Sub triggers the Cloud Run service to convert CSV to Parquet.
   - The output Parquet file is stored in GCS.
4. **Visualization Generation (Cloud Run - `visualisation.py`)**:
   - Triggered by the Parquet file upload.
   - Generates graphs based on dataset insights.
5. **Data Normalization (Cloud Run - `normalization.py`)**:
   - Triggered by Parquet file upload.
   - Scales the data and saves it back as a Parquet file.
6. **Prediction (Cloud Run - `prediction.py`)**:
   - Triggered by the normalized Parquet file.
   - Runs predictions and stores results in GCS.

## ⚙️ Setup Instructions
### 1️⃣ Prerequisites
Ensure you have the following installed:
- **Google Cloud SDK** (`gcloud` CLI)
- **Terraform** (for infrastructure setup)
- **Docker** (for containerized deployment)

### 2️⃣ Deploy Infrastructure (Terraform)
Run the following commands to create the necessary GCS bucket:
```bash
cd terraform
terraform init
terraform apply -auto-approve
```

### 3️⃣ Deploy Cloud Run Services
Build and deploy the services using Docker:
```bash
# Authenticate Docker with GCP
gcloud auth configure-docker

# Build & push container images
for service in conversion visualization normalization prediction; do
  docker build -t gcr.io/YOUR_PROJECT_ID/$service ./$service
  docker push gcr.io/YOUR_PROJECT_ID/$service
  gcloud run deploy $service --image gcr.io/YOUR_PROJECT_ID/$service --region asia-south1 --platform managed
done
```

### 4️⃣ Set Up Pub/Sub Triggers
Create topics and subscriptions for automatic triggering:
```bash
gcloud pubsub topics create conversion-topic
gcloud pubsub subscriptions create conversion-sub --topic=conversion-topic --push-endpoint=<Cloud Run URL>
# Repeat for other services
```

## 📊 Expected Outputs
- **Parquet files** stored in GCS after processing.
- **Visualizations** generated from the dataset.
- **Predicted results** saved in the bucket.

## 🛠 Troubleshooting
- Check Cloud Run logs:
  ```bash
  gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=service-name" --limit=50
  ```
- Verify Pub/Sub messages:
  ```bash
  gcloud pubsub subscriptions pull conversion-sub --auto-ack
  ```
- Required services are enabled in Google Cloud:
```bash
  gcloud services enable run.googleapis.com pubsub.googleapis.com storage.googleapis.com cloudbuild.googleapis.com
```


---
For further assistance, contact the project team or refer to the GCP documentation. 🚀

