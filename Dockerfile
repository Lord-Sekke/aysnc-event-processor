# Use a regular Python image instead of the Lambda runtime
FROM python:3.9-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy worker code
COPY app.py .

# Run the worker (not Lambda handler — just standard Python)
CMD ["app.main"]
