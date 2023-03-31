import boto3
from botocore.exceptions import ClientError

# create S3 and SQS clients
try:
    s3 = boto3.client('s3', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating S3 client: {e}")

sqs = boto3.client('sqs', region_name='us-east-1')

# get SQS queue URL
queue_url = 'https://sqs.us-east-1.amazonaws.com/395920577969/my-cloud'

# create list to keep track of processed images
processed_images = []

# loop to continuously poll for new messages
while True:
    # receive messages from SQS queue
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)

    # check if any messages are received
    if 'Messages' in response:
        # get message details
        message = response['Messages'][0]
        message_body = message['Body']
        receipt_handle = message['ReceiptHandle']

        # check if message is -1 to signal end of processing
        if message_body == '-1':
            print('All images processed')
            break

        # convert message body to integer
        try:
            image_index = int(message_body)
        except ValueError:
            print(f"Invalid message received: {message_body}")
            continue

        # check if image has already been processed
        if image_index in processed_images:
            print(f"Image {image_index} already processed")
            sqs.delete_message(QueueUrl=queue_url,
                               ReceiptHandle=receipt_handle)
            continue

        # download image from S3
        bucket_name = 'cs442-unr'
        object_key = f'{image_index}.jpg'  # image filename in S3
        object_type = object_key.split('.')[-1].lower()  # get file extension
        if object_type not in ['jpg', 'png']:
            print(f"Invalid file extension: {object_type}")
            sqs.delete_message(QueueUrl=queue_url,
                               ReceiptHandle=receipt_handle)
            continue

        try:
            image_object = s3.get_object(Bucket=bucket_name, Key=object_key)
        except ClientError as e:
            print(f"Error getting object {object_key}: {e}")
            sqs.delete_message(QueueUrl=queue_url,
                               ReceiptHandle=receipt_handle)
            continue

        image_content = image_object['Body'].read()

        # perform text detection on image
        rekognition = boto3.client('rekognition', region_name='us-east-1')
        response = rekognition.detect_text(Image={'Bytes': image_content})

        # extract detected text
        detected_text = ""
        for text in response['TextDetections']:
            if text['Type'] == 'LINE':
                detected_text += text['DetectedText'] + " "

        # print image name and detected text
        print(f"Image {image_index}: {detected_text}")

        # write image name and detected text to output.txt
        with open("output.txt", "a") as f:
            f.write(f"Image {image_index}: {detected_text}\n")

        # add image to processed list and delete message from queue
        processed_images.append(image_index)
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    else:
        print('No messages in queue')
