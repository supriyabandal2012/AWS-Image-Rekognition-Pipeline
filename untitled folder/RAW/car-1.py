import boto3
from botocore.exceptions import ClientError
import os


# Set up clients
try:
    s3 = boto3.client('s3', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating S3 client: {e}")

try:
    rekognition = boto3.client('rekognition', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating Rekognition client: {e}")
    # handle the error here

try:
    sqs = boto3.client('sqs', region_name='us-east-1')
except ClientError as e:
    print(f"Error creating SQS client: {e}")
    # handle the error here

# Set up variables
bucket_name = 'cs442-unr'
sqs_url = 'https://sqs.us-east-1.amazonaws.com/395920577969/my-cloud'
output_file = 'output.txt'
conf_threshold = 90.0

# Set up SQS message
sqs_message = {
    'MessageAttributes': {
        'Status': {
            'DataType': 'String',
            'StringValue': 'Processed'
        }
    },
    'MessageBody': ''
}

# Get list of objects in S3 bucket
response = s3.list_objects_v2(Bucket=bucket_name)

# Initialize counters
num_images = len(response['Contents'])
num_cars_detected = 0
num_images_downloaded = 0

# Open output file for writing
with open(output_file, 'w') as f:
    f.write(f"Total number of images: {num_images}\n")

# Loop through each object in S3 bucket
for obj in response['Contents']:
    obj_key = obj['Key']

    # Skip object if it's not an image
    if not obj_key.endswith('.jpg') and not obj_key.endswith('.png'):
        continue

    # Initialize variables for image processing
    img_name = os.path.splitext(obj_key)[0]
    img_ext = os.path.splitext(obj_key)[1]
    img_local_path = f"/home/ec2-user/{img_name}{img_ext}"
    cars_detected = []

    try:
        # Download image from S3 bucket
        s3.download_file(bucket_name, obj_key, img_local_path)

        # Detect objects in image
        with open(img_local_path, 'rb') as img_file:
            response = rekognition.detect_labels(
                Image={'Bytes': img_file.read()}, MinConfidence=conf_threshold)

        # Parse detection results
        for label in response['Labels']:
            if label['Name'] == 'Car' and label['Confidence'] >= conf_threshold:
                cars_detected.append(label['Name'])

        # Update counters and output file
        if len(cars_detected) > 0:
            num_cars_detected += len(cars_detected)
            num_images_downloaded += 1
            with open(output_file, 'a') as f:
                f.write(f"Image: {img_name}{img_ext}\n")
                f.write(f"Cars detected: {', '.join(cars_detected)}\n")
        else:
            os.remove(img_local_path)

    except Exception as e:
        print(f"Error processing object: {obj_key}. Exception: {e}")

    # Send SQS message if all images have been processed
    if num_images_downloaded == num_images:
        sqs.send_message(
            QueueUrl=sqs_url,
            MessageBody='All images processed',
            MessageAttributes=sqs_message['MessageAttributes']
        )

# Write final results to output file
with open(output_file, 'a') as f:
    f.write(f"Number of images downloaded: {num_images_downloaded}\n")
    f.write(f"Number of cars detected: {num_cars_detected}\n")
