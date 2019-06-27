import pymongo

try:
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    print("Successful connection to mongodb")
except Exception as e:
    print("Unsuccessful Connection to mongodb")