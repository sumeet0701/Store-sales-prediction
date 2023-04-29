import os.path
import pymongo
import pandas as pd
from os import listdir
import shutil
from application_logging.logger import App_Logger

class mongodbOperation:

    def __init__(self, logging_db):
        self.client = pymongo.MongoClient(
            "mongodb://localhost:27017/?readPreference=primary&ssl=false&directConnection=true")
        self.logging_db=logging_db
        self.logging=App_Logger()

    def insertIntoDatabase(self,  collection_name):
        '''
                    Method Name: insertIntoDatabase
                    Description: It inserts data into database
                    Output: None
                    On  Failure: Raise Exception

                    Written by: Amit Ranjan Sahoo
                    Version: 1.0
                    Revision: None
                '''
        logging_collection = 'DbInsertLog'

        goodFilePath = "Prediction_Raw_Files_Validated/Good_Raw"
        badFilePath = "Prediction_Raw_Files_Validated/Bad_Raw"
        onlyfiles = [f for f in listdir(goodFilePath)]

        database_name = 'Prediction_dataset'
        database = self.client[database_name]
        collection = database[collection_name]

        if database_name in self.client.list_database_names():
            if collection_name in database.list_collection_names():
                collection.drop()
                collection = database[collection_name]

        for file in onlyfiles:
            try:
                df = pd.read_csv(goodFilePath + '/' + file)
                for i, row in df.iterrows():
                    collection.insert_one(dict(row))
                self.logging.log(self.logging_db, logging_collection, 'INFO', " %s: File loaded successfully!!" % file)

            except Exception as e:

                self.logging.log(self.logging_db, logging_collection, 'ERROR', "Error while inserting into table: %s " % e)
                shutil.move(goodFilePath + '/' + file, badFilePath)
                self.logging.log(self.logging_db, logging_collection, 'ERROR',
                                "File Moved to Prediction_Raw_files_validated/Bad_Raw Successfully %s" % file)

    def extractDataFromDatabaseIntoCSV(self, collection_name):
        '''
                            Method Name: extractDataFromDatabaseIntoCSV
                            Description: It extracts data from database and stores it in a csv file
                            Output: Path of the csv file
                            On  Failure: Raise Exception

                            Written by: Amit Ranjan Sahoo
                            Version: 1.0
                            Revision: None
                        '''
        logging_collection = 'ExportToCsv'
        try:
            if not os.path.isdir('Prediction_FileFromDB'):
                os.mkdir('Prediction_FileFromDB')

            database_name = 'Prediction_dataset'
            database = self.client[database_name]
            collection = database[collection_name]

            df = pd.DataFrame(collection.find({}, {'_id': 0}))
            path = 'Prediction_FileFromDB/'+collection_name+'.csv'
            df.to_csv(path, header=True, index=None)
            self.logging.log(self.logging_db, logging_collection, 'INFO', 'Extracted data from Database successfully!!')

            return path
        except Exception as e:
            self.logging.log(self.logging_db, logging_collection, 'ERROR', f"Error occured to extract data from database: {e}")
            raise e