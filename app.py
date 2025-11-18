import os
import time
import json
import logging
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal

def convert_float(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: convert_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_float(v) for v in obj]
    return obj


# ----------------------------------------
# Logging Setup
# ----------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger()

# ----------------------------------------
# AWS Clients
# ----------------------------------------
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")
comprehend = boto3.client("comprehend")

QUEUE_NAME = os.getenv("QUEUE_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")
PROCESSING_DELAY = int(os.getenv("PROCESSING_DELAY", "5"))  # Simulate long jobs
FAIL_MODE = os.getenv("FAIL_MODE", "false").lower() == "true"

# ----------------------------------------
# Helpers
# ----------------------------------------
def get_queue_url(name):
    logger.info(f"Fetching URL for queue: {name}")
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]


def update_status(job_id, status, results=None):
    """Updates the DynamoDB job record."""
    table = dynamodb.Table(TABLE_NAME)

    item = {
        "id": job_id,
        "status": status
    }

    if results:
        item["results"] = results

    table.put_item(Item=item)
    logger.info(f"Updated DynamoDB item for job {job_id} → {status}")


def process_text_with_comprehend(text):
    """Process the text using AWS Comprehend."""
    try:
        response = comprehend.detect_sentiment(
            Text=text,
            LanguageCode="en"
        )
        return {
            "sentiment": response["Sentiment"],
            "scores": response["SentimentScore"]
        }
    except ClientError as e:
        logger.error(f"Comprehend error: {e}")
        return {"error": "Comprehend failed"}


def handle_message(msg):
    """Main worker logic for each job."""
    body = json.loads(msg["Body"])
    job_id = msg.get("MessageAttributes", {}).get("id", {}).get("StringValue", None)

    if not job_id:
        job_id = "unknown"

    logger.info(f"Received job: {job_id}")

    text = body.get("text", "")

    # Optional forced failure for DLQ testing
    if FAIL_MODE:
        logger.error(f"Forced failure enabled. Simulating failure for job {job_id}.")
        raise Exception("Simulated processing failure")

    # Mark job as processing
    update_status(job_id, "processing")

    # Simulate long-running job
    logger.info(f"Processing job {job_id}: sleeping for {PROCESSING_DELAY} seconds")
    time.sleep(PROCESSING_DELAY)

    # Process with Comprehend
    results = process_text_with_comprehend(text)
    logger.info(f"Job complete for {job_id}: {results}")

    # Convert floats → Decimal for DynamoDB
    results = convert_float(results)

    # Update job: completed
    update_status(job_id, "completed", results)

    return job_id


# ----------------------------------------
# Main worker loop
# ----------------------------------------
def main():
    qurl = get_queue_url(QUEUE_NAME)

    logger.info(f"Worker started. Polling queue: {qurl}")

    while True:
        messages = sqs.receive_message(
            QueueUrl=qurl,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"],
        )

        if "Messages" not in messages:
            continue

        for msg in messages["Messages"]:
            job_id = handle_message(msg)

            # delete message
            sqs.delete_message(
                QueueUrl=qurl,
                ReceiptHandle=msg["ReceiptHandle"]
            )
            logger.info(f"Deleted job {job_id} from SQS")


if __name__ == "__main__":
    main()
