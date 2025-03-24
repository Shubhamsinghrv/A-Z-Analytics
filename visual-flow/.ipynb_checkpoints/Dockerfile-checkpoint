# Use official Python image
FROM python:3.9

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt .
COPY visualisation.py .
COPY server.py .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the correct port for Cloud Run
ENV PORT=8080

# Start the Streamlit server
CMD ["python", "server.py"]
