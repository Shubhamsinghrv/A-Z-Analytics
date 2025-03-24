import streamlit as st
import pandas as pd
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from google.cloud import storage

# Load dataset from GCS
BUCKET_NAME = "intelli-ana-bucket"
FILE_NAME = "df.parquet"
SCALED_FILE_NAME = "scaled-data.parquet"
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

# Streamlit App
st.title("Interactive Data Visualization")

# Histogram
st.sidebar.header("Histogram")
hist_column = st.sidebar.selectbox("Select a column for Histogram", numerical_columns)
bins = st.sidebar.slider("Number of bins", 5, 100, 30)
fig_hist = px.histogram(df, x=hist_column, nbins=bins, title=f"Histogram of {hist_column}")
st.plotly_chart(fig_hist)

# Box Plot
st.sidebar.header("Box Plot")
box_column = st.sidebar.selectbox("Select a column for Box Plot", numerical_columns)
fig_box = px.box(df, y=box_column, title=f"Box Plot of {box_column}")
st.plotly_chart(fig_box)

# Scatter Plot
st.sidebar.header("Scatter Plot")
x_col = st.sidebar.selectbox("X-Axis", numerical_columns, key="scatter_x")
y_col = st.sidebar.selectbox("Y-Axis", numerical_columns, key="scatter_y")
fig_scatter = px.scatter(df, x=x_col, y=y_col, title=f"Scatter Plot: {x_col} vs {y_col}")
st.plotly_chart(fig_scatter)

# Sunburst Chart
st.sidebar.header("Sunburst Chart")
if len(categorical_columns) >= 2:
    hierarchy_cols = st.sidebar.multiselect("Select hierarchy columns", categorical_columns, default=categorical_columns[:2])
    metric_col = st.sidebar.selectbox("Select metric column", numerical_columns)
    if hierarchy_cols:
        fig_sunburst = px.sunburst(df, path=hierarchy_cols, values=metric_col, color=metric_col,
                                   color_continuous_scale="Blues", title="Sunburst Chart")
        st.plotly_chart(fig_sunburst)

# Correlation Heatmap
st.sidebar.header("Correlation Heatmap")
corr_matrix = df_scaled.corr()
fig_corr = px.imshow(corr_matrix, text_auto=True, color_continuous_scale="RdBu_r",
                     labels=dict(color="Correlation"))
fig_corr.update_layout(title="Feature Correlation Heatmap")
st.plotly_chart(fig_corr)

# Grouped Bar Chart
st.sidebar.header("Grouped Bar Chart")
groupby_column = st.sidebar.selectbox("Group by", df.columns)
metric_column = st.sidebar.selectbox("Metric", numerical_columns)

grouped_df = df.groupby(groupby_column)[metric_column].mean().reset_index()
fig_grouped = px.bar(grouped_df, x=groupby_column, y=metric_column, title=f"Average {metric_column} by {groupby_column}")
st.plotly_chart(fig_grouped)
