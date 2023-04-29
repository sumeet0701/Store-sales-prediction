import os
import pandas as pd
import pickle
import shutil
from data_preprocessing import preprocessing
from model_loading.load_model import Model_Loader
from application_logging.logger import App_Logger


class prediction:

    def __init__(self, path):
        self.path = path
        self.logging = App_Logger()

    def predictionFromModel(self):
        try:
            logging_db = 'Bulk_Data_Logging'
            logging_collection = 'Prediction_Logs'

            self.logging.log(logging_db, logging_collection, 'INFO', 'Prediction Started!!')


            data = pd.read_csv(self.path)
            item_outlet = data[['Item_Identifier', 'Outlet_Identifier']]

            # Data Preprocessing
            self.logging.log(logging_db, logging_collection, 'INFO', 'Data Preprocessing started!!')
            preprocess = preprocessing.Preprocessor(logging_db, logging_collection)
            data = preprocess.dropUnnecessaryColumns(data)
            data = preprocess.editDataset(data)
            data = preprocess.encodeCategoricalValues(data)
            data = preprocess.imputeMissingValues(data)
            data = preprocess.scaleNumericalValues(data)
            self.logging.log(logging_db, logging_collection, 'INFO', 'Successful End of Data Preprocessing!!')

            # Clustering
            kmeans = pickle.load(open('Clustering/cluster.pickle', 'rb'))
            clusters = kmeans.predict(data)
            data['Cluster'] = clusters
            clusters = data['Cluster'].unique()
            data = pd.concat([item_outlet, data], axis=1)

            # Prediction From Model
            self.logging.log(logging_db, logging_collection, 'INFO', 'Prediction from model started!!')

            final_output = pd.DataFrame()
            for i in clusters:
                cluster_data = data[data['Cluster'] == i]

                item_outlet = cluster_data[['Item_Identifier', 'Outlet_Identifier']]
                item_outlet = item_outlet.reset_index(drop=True)
                cluster_data = cluster_data.drop(columns=['Cluster', 'Item_Identifier', 'Outlet_Identifier'])

                loader = Model_Loader(logging_db, logging_collection)
                model = loader.get_best_model(i)
                output = model.predict(cluster_data)
                output = pd.DataFrame(output, columns=['Item_Outlet_Sales'])
                output = pd.concat([item_outlet, output], axis=1)
                final_output = pd.concat([final_output, output], axis=0, ignore_index=True)

            self.logging.log(logging_db, logging_collection, 'INFO', 'Successful End of Prediction from Model!!')

            # Preparation of Folder to send
            self.logging.log(logging_db, logging_collection, 'INFO', 'Started preparation of folder to send!!')
            output_folder = 'Final_Output_Folder'
            if not os.path.isdir(output_folder):
                os.mkdir(output_folder)

            prediction_path = 'Final_Output_Folder/Prediction'
            if not os.path.isdir(prediction_path):
                os.mkdir(prediction_path)

            final_output.to_csv(prediction_path + "/" + "Sales_Prediction.csv", header=True, index=None)

            shutil.move('PredictionArchivedBadData', output_folder)

            shutil.make_archive(output_folder, 'zip', output_folder)
            shutil.rmtree(output_folder)
            os.remove(self.path)
            shutil.rmtree('Prediction_Batch_Files')

            self.logging.log(logging_db, logging_collection, 'INFO', 'Successful End of Preparation of folder to send!!')
            self.logging.log(logging_db, logging_collection, 'INFO', 'Successful End of Prediction')

            return output_folder + '.zip'
        except Exception as e:
            raise e
