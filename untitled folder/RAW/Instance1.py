import boto3
import csv


# # IAM user setup:
# with open('credentials.csv', 'r') as file:
#     next(file)
#     reader = csv.reader(file)

#     for line in reader:
#         access_key_id = line[3]
#         secret_access_key = line[4]

# client = boto3.client('rekognition', region_name='us-east-1'
#                       aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)


# set up connections to S3 and SQS
region_name = boto3.resource('us-east-1')
s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='image-queue')

# iterate over images in S3 bucket
bucket_name = 'cs442-unr'
bucket = s3.Bucket(bucket_name)
for obj in bucket.objects.all():
    if obj.key.endswith('.jpg') or obj.key.endswith('.png'):
        # detect objects in the image using Rekognition
        client = boto3.client('rekognition')
        response = client.detect_labels(
            Image={'S3Object': {'Bucket': bucket_name, 'Name': obj.key}})

        # add index of image to queue if a car is detected with confidence > 90%
        for label in response['Labels']:
            if label['Name'] == 'Car' and label['Confidence'] > 90:
                message = queue.send_message(MessageBody=obj.key)
                print('Added index {} to queue'.format(obj.key))

# add -1 to queue to signal end of image processing
queue.send_message(MessageBody='-1')
print('Added -1 to queue')
