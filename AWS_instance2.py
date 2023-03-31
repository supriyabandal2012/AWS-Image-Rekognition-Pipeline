# Import necessary libraries
import boto3
from botocore.exceptions import ClientError

# Create SQS, S3 and Rekognition client objects and loop through try loop to get client error:
try:
    s3 = boto3.resource('s3', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating S3 client: {e}")

try:
    sqs = boto3.client('sqs', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating SQS client: {e}")

try:
    rekognition = boto3.client('rekognition', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating rekognition client: {e}")

# Define queue and bucket names:
queue_url = 'https://sqs.us-east-1.amazonaws.com/395920577969/my-cloud'
bucket_name = 'cs442-unr'

# Loop continuously to process each message from SQS:
while True:
    # Receive messages from the SQS queue stored by instance 1:
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20
    )

    # Break out of the loop if no messages are returned, to close the loop at the end:
    if 'Messages' not in response:
        break

    # Get message details
    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    message_body = message['Body']

    # Delete the message if message_body is -1 and terminate the process:
    if message_body == '-1' and 'Messages' not in sqs.receive_message(QueueUrl=queue_url, AttributeNames=['All'], MaxNumberOfMessages=1):
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        break

    # Get the object key and check if file extension is supported
    obj_key = message_body
    file_extension = obj_key.split('.')[-1].lower()
    if file_extension not in ['jpg', 'png']:
        print(f'{obj_key} has unsupported file extension')
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        continue

    # Read the image object from S3
    try:
        image_object = s3.Object(bucket_name, obj_key)
        image_content = image_object.get()['Body'].read()
    except ClientError as e:
        print(f"Error reading object {obj_key} from S3: {e}")
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        continue

    # Detect text in the image using Rekognition
    try:
        response = rekognition.detect_text(
            Image={
                'Bytes': image_content
            }
        )
    except ClientError as e:
        print(f"Error detecting text in {obj_key}: {e}")
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        continue

    # Extract detected text from the response
    detected_text = ""
    for text in response['TextDetections']:
        if text['Type'] == 'LINE':
            detected_text += text['DetectedText'] + " "

    # Write detected text to output file if both car and text are detected
    if detected_text.strip():
        with open('output.txt', 'a') as f:
            f.write(f"Car and Text detected in {obj_key}: {detected_text}\n")
        print(f"Detected text in {obj_key}: {detected_text}")
    else:
        print(f"No car or text detected in {obj_key}")

    # Delete messages from SQS once each message is processed:
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

print('All images processed')
