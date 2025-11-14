import os, time, json, boto3, logging

# Configure logging to stdout (ECS will capture this)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

QUEUE_NAME = os.getenv("QUEUE_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")

def get_queue_url(name):
    logging.info(f"Fetching SQS queue URL for {name}")
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]

def process_message(body):
    data = json.loads(body)
    seconds = int(data.get("seconds", 1))
    logging.info(f"Processing job: sleeping for {seconds} seconds")
    time.sleep(seconds)
    result = {"status": "completed", "results": f"Job ran for {seconds}s"}
    logging.info(f"Job complete: {result}")
    return result

def update_dynamo(job_id, result):
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item={"id": job_id, **result})
    logging.info(f"Updated DynamoDB item for job {job_id}")

def main():
    qurl = get_queue_url(QUEUE_NAME)
    logging.info(f"Listening to SQS queue: {qurl}")
    while True:
        msgs = sqs.receive_message(
            QueueUrl=qurl, MaxNumberOfMessages=1, WaitTimeSeconds=10,
            MessageAttributeNames=["All"]
        )
        if "Messages" not in msgs:
            logging.debug("No messages received, continuing...")
            continue
        for m in msgs["Messages"]:
            job_id = m.get("MessageAttributes", {}).get("id", {}).get("StringValue", "unknown")
            logging.info(f"Received job ID: {job_id}")
            result = process_message(m["Body"])
            update_dynamo(job_id, result)
            sqs.delete_message(QueueUrl=qurl, ReceiptHandle=m["ReceiptHandle"])
            logging.info(f"Deleted job {job_id} from SQS")

if __name__ == "__main__":
    logging.info("Starting SQS → DynamoDB worker")
    main()
