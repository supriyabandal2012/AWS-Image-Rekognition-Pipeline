import os
import json
import time
import boto3

s3 = boto3.client('s3', region_name='us-east-1')
rekognition = boto3.client('rekognition', region_name='us-east-1')
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/395920577969/my-cloud'
project_version_arn = 'arn:aws:s3:::custom-labels-console-us-east-1-b922ba5745'

bucket_name = 'cs442-unr'
try:
    # List all objects in the S3 bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    total_images = len(response['Contents'])
    downloaded_count = 0
    detected_count = 0

    with open('output.txt', 'w') as f:
        f.write('Cars detected from images:\n')

        # Loop through each object in the bucket
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

                # Check if car is detected with >= 90% confidence
                for label in response['Labels']:
                    if label['Name'] == 'Car' and label['Confidence'] >= 90:
                        detected_count += 1
                        car_name = label['Name']
                        # Create a message with the car name detected by Rekognition
                        message_body = {
                            'Car': car_name,
                            'DetectedTimestamp': int(time.time())
                        }

                        # Send the message to the SQS queue
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

                        print(f"Car detected in {obj['Key']}: {car_name}")
                        f.write(f"{car_name}\n")

                        break

                downloaded_count += 1

            # Terminate the process if all cars have been detected
            if detected_count == total_images:
                response = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps({'Terminate': True}),
                    MessageAttributes={
                        'Index': {
                            'StringValue': '-1',
                            'DataType': 'String'
                        }
                    }
                )
                break

        # All images have been downloaded
        print(f"All images downloaded. Total downloaded: {downloaded_count}")
        f.write(f"Total images downloaded: {downloaded_count}\n")
        f.write(f"Total cars detected: {detected_count}\n")

except Exception as e:
    print(f"Error processing object: {obj['Key']}. Exception: {e}")
