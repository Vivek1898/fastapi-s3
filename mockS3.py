from fastapi import FastAPI, HTTPException, UploadFile, File
from pymongo import MongoClient
from pydantic import BaseModel
import uuid
import os
from fastapi.responses import FileResponse, JSONResponse

app = FastAPI()

# MongoDB connection
client = MongoClient("mongodb://localhost:27017")
db = client.bucketdb

class Bucket(BaseModel):
    name: str

class FileModel(BaseModel):
    bucket_name: str
    file_name: str
    file_id: str

def custom_response(message: str, data: dict, code: int):
    return JSONResponse(content={"message": message, "data": data}, status_code=code)

def custom_error_response(error: str, code: int, message: str):
    return JSONResponse(content={"error": error, "message": message}, status_code=code)

# Create a new bucket
@app.post("/buckets/")
async def create_bucket(bucket: Bucket):
    existing_bucket = db.buckets.find_one({"name": bucket.name})
    if existing_bucket:
        return custom_error_response("BucketExists", 400, "Bucket already exists")
    db.buckets.insert_one(bucket.dict())
    return custom_response("Bucket created successfully", {"name": bucket.name}, 201)

# List all buckets
@app.get("/buckets/")
async def list_buckets():
    buckets = list(db.buckets.find({},  {"_id": 0}))
    return custom_response("Buckets retrieved successfully", {"buckets": buckets}, 200)

# Upload a file to a bucket
@app.post("/buckets/{bucket_name}/files/")
async def upload_file(bucket_name: str, file: UploadFile = File(...)):
    bucket = db.buckets.find_one({"name": bucket_name})
    if not bucket:
        return custom_error_response("BucketNotFound", 404, "Bucket not found")
    
    file_id = str(uuid.uuid4())
    file_path = f"files/{bucket_name}/{file_id}"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, "wb") as f:
        f.write(file.file.read())
    
    file_record = {
        "bucket_name": bucket_name,
        "file_name": file.filename,
        "file_id": file_id
    }
    
    db.files.insert_one(file_record)
    return custom_response("File uploaded successfully", {"file_id": file_id}, 201)

# List all files in a bucket
@app.get("/buckets/{bucket_name}/files/")
async def list_files(bucket_name: str):
    files = list(db.files.find({"bucket_name": bucket_name}, {"_id": 0}))
    return custom_response("Files retrieved successfully", {"files": files}, 200)

# Download a file from a bucket
@app.get("/buckets/{bucket_name}/files/{file_id}")
async def download_file(bucket_name: str, file_id: str):
    file_record = db.files.find_one({"bucket_name": bucket_name, "file_id": file_id})
    if not file_record:
        return custom_error_response("FileNotFound", 404, "File not found")
    
    file_path = f"files/{bucket_name}/{file_id}"
    if not os.path.exists(file_path):
        return custom_error_response("FileNotOnDisk", 404, "File not found on disk")
    
    return FileResponse(file_path, media_type="application/octet-stream", filename=file_record["file_name"])

# Delete a file from a bucket
@app.delete("/buckets/{bucket_name}/files/{file_id}")
async def delete_file(bucket_name: str, file_id: str):
    file_record = db.files.find_one({"bucket_name": bucket_name, "file_id": file_id})
    if not file_record:
        return custom_error_response("FileNotFound", 404, "File not found")
    
    file_path = f"files/{bucket_name}/{file_id}"
    if os.path.exists(file_path):
        os.remove(file_path)
    
    db.files.delete_one({"bucket_name": bucket_name, "file_id": file_id})
    return custom_response("File deleted successfully", {"file_id": file_id}, 200)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
