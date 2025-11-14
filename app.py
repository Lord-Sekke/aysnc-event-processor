import os, time, json, boto3
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

QUEUE_NAME = os.getenv("QUEUE_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")

def get_queue_url(name):
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]

def process_message(body):
    data = json.loads(body)
    seconds = int(data.get("seconds", 1))
    time.sleep(seconds)
    return {"status": "completed", "results": f"Job ran for {seconds}s"}

def update_dynamo(job_id, result):
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item={"id": job_id, **result})

def main():
    qurl = get_queue_url(QUEUE_NAME)
    while True:
        msgs = sqs.receive_message(
            QueueUrl=qurl, MaxNumberOfMessages=1, WaitTimeSeconds=10,
            MessageAttributeNames=["All"]
        )
        if "Messages" not in msgs: continue
        for m in msgs["Messages"]:
            job_id = m.get("MessageAttributes", {}).get("id", {}).get("StringValue", "unknown")
            result = process_message(m["Body"])
            update_dynamo(job_id, result)
            sqs.delete_message(QueueUrl=qurl, ReceiptHandle=m["ReceiptHandle"])

if __name__ == "__main__":
    main()
