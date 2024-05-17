from flask import Flask, request, Response
from enum import Enum
from PIL import Image, ImageFile
from flask_cors import CORS
import uuid
import boto3
import os
import redis
import jwt
import threading
import shutil
from config import *
import json
from io import BytesIO
ImageFile.LOAD_TRUNCATED_IMAGES = True

AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION=os.getenv('AWS_REGION')
REDIS_HOST=os.getenv('REDIS_HOST')
REDIS_PORT=os.getenv('REDIS_PORT')
REDIS_PASSWORD=os.getenv('REDIS_PASSWORD')
REDIS_TLS=os.getenv('REDIS_TLS')
SECRET_KEY=os.getenv('SECRET_KEY')

s3Client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
sqsClient = boto3.client('sqs', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
push_queue_url = 'https://sqs.us-east-2.amazonaws.com/046958462189/InputImageParts.fifo'
upload_bucket = 'dist-image-processing-pending'
download_bucket = 'dist-image-processing-finished'

r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), password=REDIS_PASSWORD, ssl=bool(int(REDIS_TLS)), decode_responses=True)

print("Connection to Redis", r.ping())

def divide_image(image, n, padding_w, padding_h):
    width_old, height_old = image.size

    aspect_ratio = width_old / height_old
    height = height_old - height_old % n
    width = int(height * aspect_ratio)

    image = image.resize((width, height))

    tmp = Image.new(image.mode, (width + 2 * padding_w, height + 2 * padding_h), (0, 0, 0))
    tmp.paste(image, (padding_w, padding_h))
    image = tmp
    chunk_height = height // n
    chunk_width = width // n

    chunks = []

    for i in range(n):
        for j in range(n):
            top = i * chunk_height + padding_h
            bottom = (i + 1) * chunk_height + padding_h
            left = j * chunk_width + padding_w
            right = (j + 1) * chunk_width + padding_w
            chunk = image.crop((left-padding_w, top-padding_h, right+padding_w, bottom+padding_h))
            chunks.append(chunk)
    return chunks, n, width, height, padding_w, padding_h


class Operations(Enum):
    BLUR = 1
    SHARPEN = 2
    EDGE_DETECTION = 3
    EMBOSS = 4
    MEDIAN = 5

app: Flask = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/api/v1/healthcheck')
def healthcheck():
    return 'OK'

@app.get('/api/v1/image/<request_id>/<extension>')
def get_image(request_id, extension):
    if r.sismember('finished', request_id):
        url = s3Client.generate_presigned_url('get_object', Params={'Bucket': download_bucket, 'Key': f'{request_id}.{extension}'}, ExpiresIn=3600)
        return {'url': url}
    elif r.scard(f"pending:{request_id}") > 0:
        return {'error': 'Request is still processing'}
    else:
        return {'error': 'Request not found'}, 404

@app.post('/api/v1/image_processing')
def image_processing():
    temp_files = []
    for image in request.files.getlist('images'):
        extension = image.filename.split('.')[-1]
        file_data = BytesIO(image.read())
        temp_files.append((extension, file_data))
    operation = request.form.get('operation', Operations.BLUR.name)
    def process(temp_files):
        for i, tup in enumerate(temp_files):
            extension, image = tup

            img = Image.open(image)

            response = { "event": "", "progress": 0, "id": i }
            
            # Generate a unique id for the image processing request
            request_id = str(uuid.uuid4())

            # Read the image as a numpy array
            # extension = image.name.split('.')[-1]

            response['event'] = f"Image received. Dividing the image into {9} chunks."
            response['progress'] = 10
            yield json.dumps(response) + '\n'
            chunks, n, width, height, padding_w, padding_h = divide_image(img, 3, 10, 10)

            pending_chunks = set()

            tmp_folder = f'tmp-{request_id}'
            if not os.path.exists(tmp_folder):
                os.makedirs(tmp_folder)

            for i, chunk in enumerate(chunks):
                response['event'] = f"Uploading chunk {i+1}/{n*n} to the cloud."
                response['progress'] = 11 + i
                yield json.dumps(response) + '\n'
                chunk.save(f'{tmp_folder}/{request_id}-{i}.{extension}')
                pending_chunks.add(i)
                s3Client.upload_file(f'{tmp_folder}/{request_id}-{i}.{extension}', upload_bucket, f'{request_id}-{i}.{extension}')

            
            shutil.rmtree(tmp_folder)

            json_message = {
                'request_id': request_id,
                'extension': extension,
                'operation': operation,
                'padding_w': padding_w,
                'padding_h': padding_h,
                'width': width,
                'height': height,
            }

            channel = f"pending:{request_id}"
            
            pubsub = r.pubsub()
            pubsub.subscribe(channel)
            
            # Send message batch to the queue
            for i, chunk in enumerate(chunks):
                response['event'] = f"Sending chunk {i+1}/{n*n} to the queue."
                response['progress'] = 21 + i
                yield json.dumps(response) + '\n'
                json_message['chunk_id'] = i
                encoded_jwt = jwt.encode(json_message, SECRET_KEY, algorithm='HS256')
                sqsClient.send_message(
                    QueueUrl=push_queue_url,
                    MessageBody=encoded_jwt,
                    MessageGroupId=str(i%2),
                    MessageDeduplicationId=f"{request_id}+{i}",
                )

            # r.sadd(f"pending:{request_id}", *pending_chunks)
            # r.set(f"chunk_count:{request_id}", len(pending_chunks))
            
            response['event'] = f"Image processing request sent. Waiting for the processed chunks."
            response['progress'] = 30
            yield json.dumps(response) + '\n'
            while True:
                message = pubsub.get_message()
                if message is None: continue
                print(message)
                if message['type'] == 'message':
                    if message['data'] == b'0':
                        break
                    
                    message = message['data']
                    decoded = jwt.decode(message, SECRET_KEY, algorithms='HS256')
                    chunk_id = decoded['chunk_id']
                    pending_chunks.remove(chunk_id)
                    response['event'] = f"Chunk {chunk_id} processed."
                    response['progress'] = 30 + (9 - len(pending_chunks)) * 3
                    yield json.dumps(response) + '\n'
                    if len(pending_chunks) == 0:
                        response['event'] = f"All chunks processed."
                        yield json.dumps(response) + '\n'
                        pubsub.unsubscribe(channel)
                        pubsub.close()
                        break
            
            r.delete(channel)
            
            tmp_folder = f'temp-{request_id}'
            if not os.path.exists(tmp_folder):
                os.makedirs(tmp_folder)
            chunks = []
            response['event'] = f"Downloading the processed chunks."
            response['progress'] = 70
            yield json.dumps(response) + '\n'
            for i in range(n*n):
                response['event'] = f"Downloading chunk {i+1}/{n*n}."
                response['progress'] = 71 + i
                yield json.dumps(response) + '\n'
                s3Client.download_file(download_bucket, f'{request_id}-{i}.{extension}', f'{tmp_folder}/{request_id}-{i}.{extension}')
                chunks.append(Image.open(f'{tmp_folder}/{request_id}-{i}.{extension}'))
            response['event'] = f"Combining the chunks."
            response['progress'] = 85
            yield json.dumps(response) + '\n'
            img = combine_image(chunks, n, width, height, padding_w, padding_h)
            img.save(f'{tmp_folder}/{request_id}.{extension}')
            response['event'] = f"Uploading the final image."
            response['progress'] = 90
            yield json.dumps(response) + '\n'
            s3Client.upload_file(f'{tmp_folder}/{request_id}.{extension}', download_bucket, f'{request_id}.{extension}')
            shutil.rmtree(tmp_folder)
            
            response['event'] = f"Final cleanup."
            response['progress'] = 95
            yield json.dumps(response) + '\n'
            for i in range(n*n):
                s3Client.delete_object(Bucket=download_bucket, Key=f'{request_id}-{i}.{extension}')

            response['event'] = f"Generating URL."
            response['progress'] = 99
            yield json.dumps(response) + '\n'
            url = s3Client.generate_presigned_url('get_object', Params={'Bucket': download_bucket, 'Key': f'{request_id}.{extension}'}, ExpiresIn=3600)
            
            yield json.dumps({'url': url, 'progress': 100, 'id': response['id']}) + '\n'
            import time
            time.sleep(0.1)

            return {'url': url}

    response = Response(process(temp_files), mimetype='text/event-stream')
    response.headers['X-Accel-Buffering'] = 'no'
    return response

def process_chunks(decoded_jwt):
    pipeline = r.pipeline()
    pipeline.srem(f"pending:{decoded_jwt['request_id']}", decoded_jwt['chunk_id'])
    pipeline.scard(f"pending:{decoded_jwt['request_id']}")
    result = pipeline.execute()
    if result[1] == 0:
        if r.sismember(f"finished", decoded_jwt['request_id']):
            return {'error': 'Request already finished'}
        else:
            # Download the images from the download bucket
            chunk_count = r.get(f"chunk_count:{decoded_jwt['request_id']}")
            # Create tmp directory
            tmp_folder = f'temp-{decoded_jwt["request_id"]}'
            if not os.path.exists(tmp_folder):
                os.makedirs(tmp_folder)
            chunks = []
            for i in range(int(chunk_count)):
                s3Client.download_file(download_bucket, f'{decoded_jwt["request_id"]}-{i}.{decoded_jwt["extension"]}', f'{tmp_folder}/{decoded_jwt["request_id"]}-{i}.{decoded_jwt["extension"]}')
                chunks.append(Image.open(f'{tmp_folder}/{decoded_jwt["request_id"]}-{i}.{decoded_jwt["extension"]}'))
            img = combine_image(chunks, 3, decoded_jwt['width'], decoded_jwt['height'], decoded_jwt['padding_w'], decoded_jwt['padding_h'])
            # Upload the image to the download bucket
            img.save(f'{tmp_folder}/{decoded_jwt["request_id"]}.{decoded_jwt["extension"]}')
            s3Client.upload_file(f'{tmp_folder}/{decoded_jwt["request_id"]}.{decoded_jwt["extension"]}', download_bucket, f'{decoded_jwt["request_id"]}.{decoded_jwt["extension"]}')
            # Update redis to mark the request as finished
            r.sadd('finished', decoded_jwt['request_id'])
            # Remove the tmp directory
            shutil.rmtree(tmp_folder)
            # Remove the images from the download bucket
            for i in range(int(chunk_count)):
                s3Client.delete_object(Bucket=download_bucket, Key=f'{decoded_jwt["request_id"]}-{i}.{decoded_jwt["extension"]}')

@app.get('/api/v1/finished_chunk/<jwt_payload>')
def finished_chunk(jwt_payload):
    # Check if the JWT is valid and is created by the server
    try:
        decoded_jwt = jwt.decode(jwt_payload, SECRET_KEY, algorithms='HS256')
    except:
        return {'error': 'Invalid JWT'}
    thread = threading.Thread(target=process_chunks, args=(decoded_jwt,))
    thread.start()
    return {'status': 'Received'}

def combine_image(chunks, n, width, height, padding_w, padding_h):
    img = Image.new('RGB', (width, height))
    for i in range(n):
        for j in range(n):
            top = i * (height // n)
            left = j * (width // n)
            # Crop the chunk to remove the padding
            chunk = chunks[i * n + j]
            chunk = chunk.crop((padding_w, padding_h, chunk.width - padding_w, chunk.height - padding_h))
            img.paste(chunk, (left, top))
    return img

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
