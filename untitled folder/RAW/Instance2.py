import boto3
import csv


# IAM user setup:
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
queue = sqs.get_queue_by_name(QueueName='my-cloud')

# open file to write results
with open('/mnt/ebs/results.txt', 'w') as f:
    # continuously poll queue for new messages
    while True:
        messages = queue.receive_messages(WaitTimeSeconds=20)
        if len(messages) > 0:
            for message in messages:
                if message.body == '-1':
                    # exit loop when -1 is received
                    print('Received -1')
                    break

                else:
                    # download image from S3 and perform text recognition using Rekognition
                    obj = s3.Object('cs442-unr', message.body)
                    response = client.detect_text(
                        Image={'S3Object': {'Bucket': 'cs442-unr', 'Name': obj.key}})

                    # write index and text to file if both car and text are detected
                    has_car = False
                    has_text = False
                    text = ''
                    for label in response['Labels']:
                        if label['Name'] == 'Car' and label['Confidence'] > 90:
                            has_car = True
                        if label['Type'] == 'WORD':
                            has_text = True
                            text += label['DetectedText'] + ' '
                    if has_car and has_text:
                        f.write('{}: {}\n'.format(message.body, text))
                    print('Processed image {}'.format(message.body))
                # delete message from queue after processing
                message.delete()
        else:
            # wait for more messages
            print('No messages received')
