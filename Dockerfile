# Use a regular Python image instead of the Lambda runtime
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the worker code
COPY app.py .

# Environment variables (will be overridden by ECS task definition)
ENV QUEUE_NAME=jobs_queue
ENV TABLE_NAME=JobsTable
ENV TIMEOUT=300

# Run the worker
CMD ["python", "app.py"]
