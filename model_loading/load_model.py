import os
import pickle
from application_logging.logger import App_Logger


class Model_Loader:
    '''
    This class shall be used to load model
    '''

    def __init__(self, logging_db, logging_collection):
        self.logging_db = logging_db
        self.logging_collection = logging_collection
        self.logging = App_Logger()

    def get_best_model(self, cluster):
        '''
        Method Name: get_best_model
        Description: It finds the model based on the given cluster and load that model
        Output: model
        On  Failure: Raise Exception

        Written by: Amit Ranjan Sahoo
        Version: 1.0
        Revision: None
        '''
        try:
            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Started to find best model')

            for file in os.listdir('Models'):
                modelName=file.split('.')[0]
                if modelName.endswith(str(cluster)):
                    model=pickle.load(open(f'Models/{file}', 'rb'))
                    break
            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Got best model and returned the best model successfully!!')

            return model

        except Exception as e:
            self.logging.log(self.logging_db, self.logging_collection, 'ERROR', f"Error occured to get best model: {e}")

            raise e