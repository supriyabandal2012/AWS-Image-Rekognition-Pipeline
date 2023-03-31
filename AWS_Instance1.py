import boto3
from botocore.exceptions import ClientError


# Create SQS and S3  client objects and loop through try loop to get client error:
try:
    s3 = boto3.resource('s3', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating S3 client: {e}")

try:
    sqs = boto3.client('sqs', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating SQS client: {e}")

# get SQS queue URL
queue_url = 'https://sqs.us-east-1.amazonaws.com/395920577969/my-cloud'

# get list of object keys in S3 bucket
bucket_name = 'cs442-unr'
bucket = s3.Bucket(bucket_name)
object_list = [obj.key for obj in bucket.objects.all()]

# process images in the bucket
for i, obj_key in enumerate(object_list):
    # determine file extension
    file_extension = obj_key.split('.')[-1].lower()
    if file_extension not in ['jpg', 'png']:
        continue

    # detect objects in image
    response = boto3.client('rekognition', region_name='us-east-1').detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': obj_key
            }
        },
        MinConfidence=90
    )

    # check if car is detected and send index to SQS
    car_detected = False
    for label in response['Labels']:
        if label['Name'] == 'Car' and label['Confidence'] >= 90:
            print(f'Car detected in {obj_key}')
            sqs.send_message(QueueUrl=queue_url, MessageBody=obj_key)
            car_detected = True
            break

    if not car_detected:
        print(f'No car detected in {obj_key}')

    print(f'{i+1}/{len(object_list)} images processed')

# send message to SQS indicating all images have been processed and terminate the loop:
sqs.send_message(QueueUrl=queue_url, MessageBody='-1')
