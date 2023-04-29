from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from dBOperation.mongoDB import mongodbOperation
from application_logging import logger

class pred_validation:
    def __init__(self,path):
        self.db = 'Bulk_Data_Logging'
        self.collection = 'Prediction_Logs'
        self.raw_data = Prediction_Data_validation(path, self.db)
        self.dBOperation = mongodbOperation(self.db)
        self.log_writer = logger.App_Logger()

    def prediction_validation(self):

        try:

            self.log_writer.log(self.db, self.collection, 'INFO', 'Start of Validation on files for prediction!!')
            #extracting values from prediction schema
            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,noofcolumns = self.raw_data.valuesFromSchema()
            #getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            #validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            #validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            #validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.db, self.collection, 'INFO',"Raw Data Validation Complete!!")

            self.log_writer.log(self.db, self.collection, 'INFO',"Insertion of Data into Database started!!!!")
            #insert csv files in the Database
            self.dBOperation.insertIntoDatabase('Good_Raw_Data')
            self.log_writer.log(self.db, self.collection, 'INFO',"Insertion in Database completed!!!")
            self.log_writer.log(self.db, self.collection, 'INFO',"Deleting Good Data Folder!!!")
            #Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataPredictionFolder()
            self.log_writer.log(self.db, self.collection, 'INFO',"Good_Data folder deleted!!!")
            self.log_writer.log(self.db, self.collection, 'INFO',"Moving bad files to Archive and deleting Bad_Data folder!!!")
            #Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.db, self.collection, 'INFO',"Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.db, self.collection, 'INFO',"Validation Operation completed!!")
            self.log_writer.log(self.db, self.collection, 'INFO',"Extracting csv file from collection")
            #export data in database to csvfile
            extracted_data_path = self.dBOperation.extractDataFromDatabaseIntoCSV('Good_Raw_Data')
            return extracted_data_path

        except Exception as e:
            raise e









