# Use Python base image
FROM python:3.9-slim

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy worker code
COPY app.py .

# Start worker
CMD ["python", "app.py"]
