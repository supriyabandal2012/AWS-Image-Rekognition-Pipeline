import boto3
from botocore.exceptions import ClientError
import json
import time
import os


# set up a clients and use try block to handle error:
try:
    s3 = boto3.client('s3', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating S3 client: {e}")

try:
    rekognition = boto3.client('rekognition', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating Rekognition client: {e}")


try:
    sqs = boto3.client('sqs', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating SQS client: {e}")


queue_url = 'https://sqs.us-east-1.amazonaws.com/395920577969/my-cloud'
bucket_name = 'cs442-unr'

try:
    # List all objects in the S3 bucket
    response = s3.list_objects_v2(Bucket=bucket_name)

    # Loop through each object in the bucket
    downloaded_count = 0
    text_detected_count = 0
    with open('output.txt', 'w') as f:
        for obj in response['Contents']:
            # Get the file extension of the object
            file_extension = os.path.splitext(obj['Key'])[1]

            # Check if the file extension is .png or .jpg
            if file_extension.lower() in ['.png', '.jpg']:
                # Call the appropriate method to detect text
                response = rekognition.detect_text(
                    Image={
                        'S3Object': {
                            'Bucket': bucket_name,
                            'Name': obj['Key']
                        }
                    }
                )

                # Get the list of text detected by Rekognition
                text_list = []
                for text in response['TextDetections']:
                    if text['Type'] == 'LINE':
                        text_list.append(text['DetectedText'])

                # Create a message with the text detected by Rekognition
                message_body = {
                    'Text': text_list,
                    'DetectedTimestamp': int(time.time())
                }

                # Send the message to the SQS queue if any text is detected
                if text_list:
                    response = sqs.send_message(
                        QueueUrl=queue_url,
                        MessageBody=json.dumps(message_body),
                        MessageAttributes={
                            'ImageName': {
                                'StringValue': obj['Key'],
                                'DataType': 'String'
                            }
                        }
                    )
                    text_detected_count += 1
                    f.write(f"Text detected in {obj['Key']}: {text_list}\n")

                downloaded_count += 1

        # All images have been downloaded
        f.write(
            f"All images downloaded. Total downloaded: {downloaded_count}\n")
        f.write(f"Total images with text detected: {text_detected_count}\n")
        if text_detected_count == downloaded_count:
            # If all downloaded images have text detected, send index -1 to SQS to terminate the process
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody='-1',
                MessageAttributes={
                    'TerminationIndex': {
                        'StringValue': '-1',
                        'DataType': 'String'
                    }
                }
            )
            f.write("Termination index sent to SQS.\n")

except Exception as e:
    print(f"Error processing object: {obj['Key']}. Exception: {e}\n")
