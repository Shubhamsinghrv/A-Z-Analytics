import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from google.cloud import storage

# Load dataset from GCS
BUCKET_NAME = "intelli-ana-bucket"
FILE_NAME = "df.parquet"
SCALED_FILE_NAME = "scaled-data.parquet"
VISUALS_FOLDER = "visuals/"
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

blob = bucket.blob(FILE_NAME)
blob.download_to_filename(FILE_NAME)
df = pd.read_parquet(FILE_NAME)

blob = bucket.blob(SCALED_FILE_NAME)
blob.download_to_filename(SCALED_FILE_NAME)
df_scaled = pd.read_parquet(SCALED_FILE_NAME)

# Standardize column names
df.columns = df.columns.str.replace(r"[^\w\s]", "", regex=True).str.replace(" ", "_")
df_scaled.columns = df_scaled.columns.str.replace(r"[^\w\s]", "", regex=True).str.replace(" ", "_")

# Identify categorical and numerical columns
categorical_columns = df.select_dtypes(exclude="number").columns.tolist()
numerical_columns = df.select_dtypes(include="number").columns.tolist()

def save_and_upload_plot(filename):
    plt.savefig(filename)
    blob = bucket.blob(VISUALS_FOLDER + filename)
    blob.upload_from_filename(filename)
    plt.close()

# Streamlit App
st.title("DASHBOARD")

# Histogram
st.header("Histogram")
hist_column = numerical_columns[0]  # Default to the first numerical column
plt.figure(figsize=(8, 5))
sns.histplot(df[hist_column], bins=30, kde=True)
plt.title(f"Histogram of {hist_column}")
save_and_upload_plot("histogram.png")
st.image("histogram.png")

# Box Plot
st.header("Box Plot")
box_column = numerical_columns[0]
plt.figure(figsize=(8, 5))
sns.boxplot(y=df[box_column])
plt.title(f"Box Plot of {box_column}")
save_and_upload_plot("boxplot.png")
st.image("boxplot.png")

# Scatter Plot
st.header("Scatter Plot")
x_col = numerical_columns[0]
y_col = numerical_columns[1] if len(numerical_columns) > 1 else numerical_columns[0]
plt.figure(figsize=(8, 5))
sns.scatterplot(x=df[x_col], y=df[y_col])
plt.title(f"Scatter Plot: {x_col} vs {y_col}")
save_and_upload_plot("scatterplot.png")
st.image("scatterplot.png")

# Correlation Heatmap
st.header("Correlation Heatmap")
corr_matrix = df_scaled.corr()
plt.figure(figsize=(10, 6))
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f")
plt.title("Feature Correlation Heatmap")
save_and_upload_plot("correlation_heatmap.png")
st.image("correlation_heatmap.png")

# Grouped Bar Chart
st.header("Grouped Bar Chart")

# Ensure categorical and numerical columns exist
if categorical_columns and numerical_columns:
    groupby_column = categorical_columns[0]
    metric_column = numerical_columns[0]
    # Check if columns exist in df
    if groupby_column in df.columns and metric_column in df.columns:
        grouped_df = df.groupby(groupby_column)[metric_column].mean().reset_index()
        # Sort values for better visualization
        grouped_df = grouped_df.sort_values(by=metric_column, ascending=False)
        # Plot
        plt.figure(figsize=(12, 6))
        sns.barplot(x=groupby_column, y=metric_column, data=grouped_df, palette="viridis")
        # Improve readability
        plt.xticks(rotation=45, ha="right")
        plt.title(f"Average {metric_column} by {groupby_column}")
        plt.xlabel(groupby_column)
        plt.ylabel(f"Average {metric_column}")
        # Save and display
        save_and_upload_plot("grouped_bar_chart.png")
        st.image("grouped_bar_chart.png")
    else:
        st.error(f"Columns '{groupby_column}' or '{metric_column}' not found in DataFrame.")
else:
    st.error("No categorical or numerical columns available.")

