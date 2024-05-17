#!/home/mustafa/DevEnv/dist-flask-app/venv/bin/python3
import boto3
import os
import cv2
import jwt
import numpy as np
import redis
from enum import Enum
from config import *

AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION=os.getenv('AWS_REGION')
REDIS_HOST=os.getenv('REDIS_HOST')
REDIS_PORT=os.getenv('REDIS_PORT')
REDIS_TLS=os.getenv('REDIS_TLS')
REDIS_PASSWORD=os.getenv('REDIS_PASSWORD')

s3Client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
sqsClient = boto3.client('sqs', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

push_queue_url = 'https://sqs.us-east-2.amazonaws.com/046958462189/InputImageParts.fifo'
upload_bucket = 'dist-image-processing-pending'
download_bucket = 'dist-image-processing-finished'

r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), password=REDIS_PASSWORD, ssl=bool(int(REDIS_TLS)), decode_responses=True)

print("Connection to Redis", r.ping())

print("Starting to receive messages from pid %d" % os.getpid())

counter = [0]

class Operations(Enum):
    BLUR = 1
    SHARPEN = 2
    EDGE_DETECTION = 3
    EMBOSS = 4
    MEDIAN = 5

def main():
    # If the folder tmp does not exist, create it
    if not os.path.exists("tmp"):
        os.makedirs("tmp")
    while True:
        counter[0] += 1
        print("Checking for messages..." + str(counter[0]))
        response = sqsClient.receive_message(
            QueueUrl=push_queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            WaitTimeSeconds=5
        )
        if len(response.get('Messages', [])) == 0:
            # Go Up one line in the console
            print("\033[F", end="")
            continue
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        print(f"PID {os.getpid()}: Received message {message['Body']}")
        jwt_token = message['Body']
        message = jwt.decode(jwt_token, options={"verify_signature": False}, algorithms='HS256')
        request_id = message['request_id']
        chunk_id = message['chunk_id']
        extension = message['extension']
        
        file_name = f"{request_id}-{chunk_id}.{extension}"
        download_image(file_name)
        operation = message['operation']
        padding_w = message['padding_w']
        padding_h = message['padding_h']
        
        kernel_size_w = padding_w * 2 + 1
        kernel_size_h = padding_h * 2 + 1
        
        image = cv2.imread(f"tmp/{file_name}")
        image = process_image(image, operation, kernel_size_w, kernel_size_h)
        cv2.imwrite(f"tmp/{file_name}", image)
        
        upload_image(file_name)
        delete_image(file_name)
        delete_image_s3(file_name)
        sqsClient.delete_message(
            QueueUrl=push_queue_url,
            ReceiptHandle=receipt_handle
        )
        

        # Check if the channel exists
        # if not r.exists(f"pending:{request_id}"):
        #     continue
        r.publish(f"pending:{request_id}", jwt_token)        


def process_image(image, operation, kernel_size_w, kernel_size_h):
    if operation == Operations.BLUR.name:
        return cv2.GaussianBlur(image, (kernel_size_w, kernel_size_h), 0)
    elif operation == Operations.SHARPEN.name:
        # Create a kernel filled with -1
        # kernel = np.ones((kernel_size_w, kernel_size_h), np.float32) * -1
        # center = kernel_size_h * kernel_size_w
        # kernel[kernel_size_w // 2, kernel_size_h // 2] = center
        kernel = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]])
        return cv2.filter2D(image, -1, kernel)
    elif operation == Operations.EDGE_DETECTION.name:
        return cv2.Canny(image, 100, 200)
    elif operation == Operations.EMBOSS.name:
        kernel = np.zeros((kernel_size_w, kernel_size_h), np.float32)
        kernel[kernel_size_w // 2, kernel_size_h // 2] = 1
        
        kernel[kernel_size_w // 2, kernel_size_h // 2 + 1] = 1
        kernel[kernel_size_w // 2 + 1, kernel_size_h // 2] = 1
        kernel[kernel_size_w // 2 + 1, kernel_size_h // 2 + 1] = 2
        
        kernel[kernel_size_w // 2, kernel_size_h // 2 - 1] = -1
        kernel[kernel_size_w // 2 - 1, kernel_size_h // 2] = -1
        kernel[kernel_size_w // 2 - 1, kernel_size_h // 2 - 1] = -2
        
        return cv2.filter2D(image, -1, kernel)
    elif operation == Operations.MEDIAN.name:
        return cv2.medianBlur(image, min(kernel_size_w, kernel_size_h))
    else:
        random_operation = np.random.choice(list(Operations))
        return process_image(image, random_operation, kernel_size_w, kernel_size_h)

def download_image(file_name):
    s3Client.download_file(upload_bucket, file_name, f"tmp/{file_name}")

def delete_image_s3(file_name):
    s3Client.delete_object(Bucket=upload_bucket, Key=file_name)

def upload_image(file_name):
    s3Client.upload_file(f"tmp/{file_name}", download_bucket, file_name)

def delete_image(file_name):
    os.remove(f"tmp/{file_name}")

if __name__ == '__main__':
    main()
