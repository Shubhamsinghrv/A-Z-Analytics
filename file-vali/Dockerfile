# Use official Python image
FROM python:3.9

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y libgomp1 && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY validation.py .

# Expose the correct port
ENV PORT=5000

# Run the Flask app
CMD ["python", "validation.py"]
