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
project_version_arn = 'arn:aws:s3:::custom-labels-console-us-east-1-b922ba5745'

bucket_name = 'cs442-unr'

try:
    # List all objects in the S3 bucket
    response = s3.list_objects_v2(Bucket=bucket_name)

    # Loop through each object in the bucket
    downloaded_count = 0
    car_detected_count = 0
    with open('output.txt', 'w') as f:
        for obj in response['Contents']:
            # Get the file extension of the object
            file_extension = os.path.splitext(obj['Key'])[1]

            # Check if the file extension is .png or .jpg
            if file_extension.lower() in ['.png', '.jpg']:
                # Call the appropriate method to detect custom labels or general labels
                if file_extension.lower() == ['.png', '.jpg']:
                    response = rekognition.detect_custom_labels(
                        Image={
                            'S3Object': {
                                'Bucket': bucket_name,
                                'Name': obj['Key']
                            }
                        },
                        ProjectVersionArn=project_version_arn,
                        MinConfidence=90
                    )
                else:
                    response = rekognition.detect_labels(
                        Image={
                            'S3Object': {
                                'Bucket': bucket_name,
                                'Name': obj['Key']
                            }
                        },
                        MaxLabels=10,
                        MinConfidence=90
                    )

                # Get the list of car labels detected by Rekognition
                car_labels = []
                if file_extension.lower() == ['.png', '.jpg']:
                    for label in response['CustomLabels']:
                        if label['Name'].lower() == 'car' and label['Confidence'] >= 90:
                            car_labels.append(label['Name'])
                else:
                    for label in response['Labels']:
                        if label['Name'].lower() == 'car' and label['Confidence'] >= 90:
                            car_labels.append(label['Name'])

                # Create a message with the car labels detected by Rekognition
                message_body = {
                    'Cars': car_labels,
                    'DetectedTimestamp': int(time.time())
                }

                # Send the message to the SQS queue if any car is detected
                if car_labels:
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
                    car_detected_count += 1
                    f.write(f"Cars detected in {obj['Key']}: {car_labels}\n")

                downloaded_count += 1

        # All images have been downloaded
        f.write(
            f"All images downloaded. Total downloaded: {downloaded_count}\n")
        f.write(f"Total images with car detected: {car_detected_count}\n")
        if car_detected_count == downloaded_count:
            # If all downloaded images have car detected, send index -1 to SQS to terminate the process
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
