import datetime as dt
import uuid
from pymongo.mongo_client import MongoClient
from google.cloud import storage
from fastapi import FastAPI, File, UploadFile, Form
from typing import Annotated, Union
from bson import ObjectId
from fastapi.responses import Response
from bson.json_util import dumps, loads 
import json
app = FastAPI()
storage_client = storage.Client()
bucket_name = "project_nube"
bucket = storage_client.get_bucket(bucket_name)

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)


def storage_write(blob_name, content, content_type):
    """Write and read a blob from GCS using file-like IO"""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(content, content_type= content_type)
    #with blob.open("r") as f:
    #    print(f.read())

    #with blob.open("w") as f:
    #    f.write(content)

def storage_delete(blob_name):
     blob = bucket.blob(blob_name)
     blob.delete()

def storage_read(blob_name) -> bytes:
    blob = bucket.blob(blob_name)
    return blob.download_as_bytes()

class Connection:
    def __init__(self, connectionsString, dbName):
        self.connectionString = connectionsString
        self.dbName = dbName
        # Create a new client and connect to the server
        client = MongoClient(connectionsString)
        # Send a ping to confirm a successful connection
        try:
            client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)

        self.DB = client[dbName]

class FileData:
    def __init__(self,_id, name, extension, date, size, note_id, content_type) -> None:
        self._id = _id
        self.name = name
        self.extension = extension
        self.date = date
        self.size = size
        self.note_id = note_id
        self.content_type = content_type

class RepositoryFiles:
    def __init__(self, connection: Connection):
        self.collection = connection.DB["files"]

    def insert(self, fileData: FileData):
       x = self.collection.insert_one(fileData.__dict__)
       print(x)
       return x.inserted_id

    def get(self, id: str):
        return self.collection.find_one({"_id":  ObjectId(id)})

    def getListFile(self, note_id: int):
        data = []
        cursor =  self.collection.find({"note_id":  note_id})
        for record in cursor:
            record["_id"] = str(record["_id"])
            data.append(record)
        return data

    def delete(self, id):
        query = {"_id": ObjectId(id)}
        self.collection.delete_one(query)

colfiles = RepositoryFiles(Connection("mongodb+srv://dariveraca:123@projectnube.uameh7i.mongodb.net/?retryWrites=true&w=majority&appName=ProjectNube", "ProjectNube"))


@app.get("/files/list/{note_id}")
async def get_file(note_id: int):
    return colfiles.getListFile(note_id)

@app.get("/files/download/{id}")
async def get_file(id: str):
    file = colfiles.get(id)
    if file is None: 
        return Response(content= "Not found", status_code= 404)

    data = storage_read(id)
    return Response(content=data, media_type=file.get("content_type"))

@app.post("/files/upload/{note_id}")
async def create_file(
    note_id: int,
    file: Annotated[UploadFile, File()]):

    fileData = await file.read()
    idFile = ObjectId()
    colfiles.insert(fileData= FileData(idFile, 
        file.filename, 
        file.filename.split('.')[1], 
        dt.datetime.now(), 
        file.size,
        note_id,
        file.content_type))

    storage_write(idFile.__str__(), fileData, file.content_type)

    return {
        "file_id": idFile.__str__()
    }

@app.delete("/files/{id}")
async def get_file(id: str):
    colfiles.delete(id)
    storage_delete(id)
    return "ok"