import pandas as pd
import numpy as np
import math
import os
from multiprocessing import Pool, cpu_count
from sklearn.model_selection import train_test_split
from sklearn.ensemble import ExtraTreesRegressor, RandomForestRegressor, AdaBoostRegressor
from sklearn.tree import DecisionTreeRegressor, ExtraTreeRegressor
from joblib import parallel_backend
import plotly.express as px
from google.cloud import storage


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    from google.cloud import storage
    import os

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    if not os.path.exists(source_file_name):
        print(f"❌ ERROR: {source_file_name} does not exist locally!")
        return
    
    print(f"⬆️ Uploading {source_file_name} to gs://{bucket_name}/{destination_blob_name}...")
    
    try:
        blob.upload_from_filename(source_file_name)
        print(f"✅ Successfully uploaded to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")


def load_data(parquet_gcs_path):
    """Load dataset from GCS"""
    return pd.read_parquet(parquet_gcs_path, storage_options={"token": "cloud"}, engine="pyarrow")

# Load dataset
PARQUET_GCS_PATH = "gs://intelli-ana-bucket/scaled-data.parquet"
df = load_data(PARQUET_GCS_PATH)

def train_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, shuffle=True
    )
    
    model = ExtraTreesRegressor(n_jobs=8, random_state=42, n_estimators=10)
    with parallel_backend("loky", n_jobs=4):
        model.fit(X_train, y_train)
    
    return model

import plotly.io as pio

def select_top_features(model, X, min_features=5, plot=True):
    if hasattr(model, "feature_importances_"):
        feature_importances = model.feature_importances_
    else:
        raise ValueError("Model does not support feature importances.")
    
    importance_df = pd.DataFrame({
        'Feature': X.columns,
        'Importance': feature_importances
    }).sort_values(by='Importance', ascending=False).reset_index(drop=True)
    
    total_features = len(importance_df)
    additional_features = math.ceil(0.6 * total_features)
    selected_features_count = min(total_features, max(min_features, additional_features + min_features))
    selected_features = importance_df.head(selected_features_count)['Feature'].tolist()
    
    if plot:
        print("📊 Generating Feature Importance Graph...")

        fig = px.bar(importance_df.head(selected_features_count), 
                     x="Importance", 
                     y="Feature", 
                     orientation="h",
                     title="Feature Importance",
                     labels={"Importance": "Importance Score", "Feature": "Features"},
                     color="Importance",
                     color_continuous_scale="blues")
        fig.update_layout(yaxis=dict(categoryorder="total ascending"))

        # Define file names
        png_filename = "feature_importance.png"
        html_filename = "feature_importance.html"

        # ✅ Check available image rendering engines
        print(f"🖼️ Available image renderers: {pio.renderers}")
        
        try:
            # Save the graph locally
            fig.write_image(png_filename)  # PNG
            fig.write_html(html_filename)  # HTML
            print(f"✅ Graphs saved locally: {png_filename}, {html_filename}")
        except Exception as e:
            print(f"❌ Error saving graph: {e}")
            return selected_features  # Exit early if saving fails

    return selected_features


def batch_train_model(X, y, batch_size=50000, min_features=5):
    # Convert datetime columns if present
    for col in X.select_dtypes(include=['datetime64']).columns:
        X[col] = X[col].astype('int64')

    models = {
        "DecisionTreeRegressor": DecisionTreeRegressor(),
        "ExtraTreeRegressor": ExtraTreeRegressor(),
        "RandomForestRegressor": RandomForestRegressor(n_estimators=10, random_state=42),
        "ExtraTreesRegressor": ExtraTreesRegressor(n_estimators=10, random_state=42),
        "AdaBoostRegressor": AdaBoostRegressor(n_estimators=10, random_state=42),
    }

    results = {}
    final_selected_features = None
    num_rows = X.shape[0]

    for model_name, model in models.items():
        batch_scores = []
        selected_features = None
        first_batch = True

        for i in range(0, num_rows, batch_size):
            X_batch = X.iloc[i:i + batch_size]
            y_batch = y.iloc[i:i + batch_size]

            model.fit(X_batch, y_batch)

            if first_batch and hasattr(model, "feature_importances_"):
                selected_features = select_top_features(model, X_batch, min_features)
                first_batch = False

            if selected_features is not None:
                X_batch_selected = X_batch[selected_features]
            else:
                X_batch_selected = X_batch

            model.fit(X_batch_selected, y_batch)
            batch_score = model.score(X_batch_selected, y_batch)
            batch_scores.append(batch_score)

        avg_score = np.mean(batch_scores)
        results[model_name] = avg_score

        if results[model_name] == max(results.values()):
            final_selected_features = selected_features

    best_model_name = max(results, key=results.get)
    best_model_score = results[best_model_name]
    best_model = models[best_model_name]

    return best_model, best_model_name, best_model_score, final_selected_features

if __name__ == "__main__":
    test_input = df.iloc[:5].drop(columns=["Healthy life\nexpectancy"]).to_dict(orient="records")
    print("✅ Script execution completed!")
