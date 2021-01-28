# Script to load json files from DIR into MongoDB Atlas.

import os
import json
from pymongo import MongoClient

print("Conneting to MongoDB Atlas ......")
client = MongoClient("mongodb+srv://")
print("Connection Sucessful ...........")
db = client.get_database('xxx_testDB')
collection = db.tracker2


path = 'xxx' # Directory path where the staging data is present


print("Total number of files: ",len(os.listdir(path)))
print("File names: ",os.listdir(path))

for file in os.listdir(path):
    try:
        with open(file) as f:
            file_data = json.load(f)
            if isinstance(file_data, list):
                collection.insert_many(file_data)   
            else: 
                collection.insert_one(file_data)
#print("File have been loaded successfully")
    except BaseException:
        print("Exception Caught")

print("Files have been loaded succesfully to MongoDB Atlas")
