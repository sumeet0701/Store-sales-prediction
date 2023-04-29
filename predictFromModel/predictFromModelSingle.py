import pandas as pd
import numpy as np
import pickle
from data_preprocessing import preprocessing
from model_loading.load_model import Model_Loader
from application_logging.logger import App_Logger

class prediction:

    def __init__(self, data):
        self.data = data
        self.logging = App_Logger()

    def predictionFromModel(self):
        try:
            logging_db='Single_Data_Logging'
            logging_collection='Prediction_Logs'

            self.logging.log(logging_db, logging_collection, 'INFO', 'Prediction Started!!')
            self.logging.log(logging_db, logging_collection, 'INFO', f"user input: {self.data}")

            data = pd.DataFrame(self.data, index=[0])

            #Data Preprocessing
            self.logging.log(logging_db, logging_collection, 'INFO', 'Data Preprocessing started!!')
            preprocess = preprocessing.Preprocessor(logging_db, logging_collection)
            data = preprocess.dropUnnecessaryColumns(data)
            data = preprocess.editDataset(data)
            data = preprocess.encodeCategoricalValues(data)
            # data=preprocess.imputeMissingValues(data)
            data = preprocess.scaleNumericalValues(data)
            self.logging.log(logging_db, logging_collection, 'INFO', 'Successful End of Data Preprocessing!!')

            #Clustering
            kmeans = pickle.load(open('Clustering/cluster.pickle', 'rb'))
            cluster=kmeans.predict(data)[0]


            #Prediction From Model
            loader = Model_Loader(logging_db, logging_collection)
            model =  loader.get_best_model(cluster)
            output = np.round(model.predict(data)[0],4)

            self.logging.log(logging_db, logging_collection, 'INFO', f"Prediction output: {output}")
            self.logging.log(logging_db, logging_collection, 'INFO', 'Successful End of Prediction')

            return output

        except Exception as e:
            raise e




