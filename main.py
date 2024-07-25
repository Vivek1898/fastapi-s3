from fastapi import FastAPI, File, UploadFile
from pymongo import MongoClient
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import boto3
import hashlib
# Creating our base app
app = FastAPI()

origins = [
    "http://localhost:3001",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def custom_response(message: str, data: dict, code: int):
    return JSONResponse(content={"message": message, "data": data}, status_code=code)


def custom_error_response(error: str, code: int, message: str):
    return JSONResponse(content={"error": error, "message": message}, status_code=code)


# S3
# S3 have their own functions for uploading and downloading files
access_key = "key"
secret_key = "key"
region = "region"
bucket_name = "bucket"
client = boto3.client('s3',
                      aws_access_key_id= access_key,
                      aws_secret_access_key=secret_key,
                      region_name="us-east-1")


@app.get("/list/files")
async def list_files():
    all_items = {
        "files": [],
        "prefixes": []
    }
    try:
        continuation_token = None
        while True:
            if continuation_token:
                response = client.list_objects_v2(
                    Bucket=bucket_name,
                    Delimiter='/',
                    ContinuationToken=continuation_token
                )
            else:
                response = client.list_objects_v2(
                    Bucket=bucket_name,
                    Delimiter='/'
                )

            contents = response.get('Contents', [])
            common_prefixes = response.get('CommonPrefixes', [])

            for item in contents:
                all_items["files"].append(item['Key'])

            for prefix in common_prefixes:
                all_items["prefixes"].append(prefix['Prefix'])

            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
            else:
                break

        return custom_response(message="Files and prefixes retrieved successfully", data=all_items, code=200)

    except Exception as e:
        return custom_error_response(error=str(e), code=500, message="Internal Server Error")

@app.post("/upload/file")
async def upload_file(file: UploadFile = File(...)):
    try:
        # we have received the file
        print(file.filename)
        response = client.upload_fileobj(file.file, "trc-debug", file.filename)
        isPresent = client.list_objects_v2(Bucket=bucket_name, Prefix=file.filename)
        files = [item['Key'] for item in isPresent.get('Contents', [])]

        if file.filename in files:
            return custom_response(message="File uploaded successfully", data={"file_name": file.filename}, code=200)
        else:
            return custom_error_response(error="File upload failed", code=500, message="Internal Server Error")

       # print(isPresent)
        # Upload file to S3
        #return custom_response(message="File uploaded successfully", data={"file_name": file.filename}, code=200)

    except Exception as e:
        return custom_error_response(error=str(e), code=500, message="Internal Server Error")


@app.get("/list/files/v2")
async def list_files():

    all_items = {
        "files": [],
        "prefixes": []
    }
    try:
        continuation_token = None
        while True:
            if continuation_token:
                response = client.list_objects_v2(
                    Bucket=bucket_name,
                    Delimiter='/',
                    ContinuationToken=continuation_token
                )
            else:
                response = client.list_objects_v2(
                    Bucket=bucket_name,
                    Delimiter='/'
                )

            contents = response.get('Contents', [])
            common_prefixes = response.get('CommonPrefixes', [])

            for item in contents:
                # Generate a public URL for each file
                file_key = item['Key']
                public_url = client.generate_presigned_url('get_object',
                                                           Params={'Bucket': bucket_name, 'Key': file_key},
                                                           ExpiresIn=3600)
                all_items["files"].append({"file_key": file_key, "public_url": public_url})

            for prefix in common_prefixes:
                all_items["prefixes"].append(prefix['Prefix'])

            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
            else:
                break

        # Recursively list files in folders
        for prefix in all_items["prefixes"]:
            continuation_token = None
            while True:
                if continuation_token:
                    response = client.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=prefix,
                        ContinuationToken=continuation_token
                    )
                else:
                    response = client.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=prefix
                    )

                contents = response.get('Contents', [])

                for item in contents:
                    # Generate a public URL for each file
                    file_key = item['Key']
                    public_url = client.generate_presigned_url('get_object',Params={'Bucket': bucket_name, 'Key': file_key}, ExpiresIn=3600)
                all_items["files"].append({"file_key": file_key, "public_url": public_url})

                if response.get('IsTruncated'):
                    continuation_token = response.get('NextContinuationToken')
                else:
                    break

        return custom_response(message="Files and prefixes retrieved successfully", data=all_items, code=200)

    except Exception as e:
        return custom_error_response(error=str(e), code=500, message="Internal Server Error")


@app.put("/update/file")
async def update_file(file_key: str, file: UploadFile = File(...)):
    try:
        # Print the filename for debugging
        print(file_key)
        # Upload file to S3 (this will overwrite the existing file)
        client.upload_fileobj(file.file, bucket_name, file_key)

        # Verify if the file was uploaded
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=file_key)
        files = [item['Key'] for item in response.get('Contents', [])]

        if file_key in files:
            return custom_response(message="File updated successfully", data={"file_name": file_key}, code=200)
        else:
            return custom_error_response(error="File update failed", code=500, message="Internal Server Error")

    except Exception as e:
        return custom_error_response(error=str(e), code=500, message="Internal Server Error")




def calculate_md5(file):
    hash_md5 = hashlib.md5()
    for chunk in iter(lambda: file.read(4096), b""):
        hash_md5.update(chunk)
    file.seek(0)  # Reset file pointer to the beginning after reading
    return hash_md5.hexdigest()


@app.put("/update/file/v2")
async def update_file(file_key: str, file: UploadFile = File(...)):
    try:
        # Calculate MD5 hash of the new file
        new_file_md5 = calculate_md5(file.file)

        # Get the ETag of the existing file from S3
        try:
            #asking for the key (head_object), then use some algos to check if file is changed or not
            response = client.head_object(Bucket=bucket_name, Key=file_key)
            existing_file_etag = response['ETag'].strip('"')
        except client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return custom_error_response(error="File not found", code=404, message="File not found in S3")
            else:
                return custom_error_response(error=str(e), code=500, message="Internal Server Error")

        # Compare the MD5 hash with the ETag
        if new_file_md5 == existing_file_etag:
            return custom_response(message="File is not changed", data={"file_name": file_key}, code=200)

        # Upload file to S3 (this will overwrite the existing file)
        file.file.seek(0)  # Reset file pointer to the beginning
        client.put_object(Bucket=bucket_name, Key=file_key, Body=file.file.read())

        # Verify if the file was uploaded
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=file_key)
        files = [item['Key'] for item in response.get('Contents', [])]

        if file_key in files:
            return custom_response(message="File updated successfully", data={"file_name": file_key}, code=200)
        else:
            return custom_error_response(error="File update failed", code=500, message="Internal Server Error")

    except Exception as e:
        return custom_error_response(error=str(e), code=500, message="Internal Server Error")