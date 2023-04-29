import pymongo
from datetime import datetime

class App_Logger:
    def __init__(self):

        self.client = pymongo.MongoClient(
            "mongodb+srv://Shivan1:Shivan140306@cluster0.nb0paq8.mongodb.net/?retryWrites=true&w=majority")




    def log(self, db, collection, tag, log_message):
        database = self.client[db]
        collection = database[collection]
        record={
            "timestamp" : str(datetime.now()),
            "tag" : tag,
            "log_message" : log_message
        }

        collection.insert_one(record)
