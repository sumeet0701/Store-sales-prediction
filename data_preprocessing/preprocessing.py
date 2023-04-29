import pandas as pd
import pickle
from sklearn.impute import KNNImputer
import numpy as np
from application_logging.logger import App_Logger

class Preprocessor:
    '''
    This class will be used to preprocess the data before prediction
    '''

    def __init__(self, logging_db, logging_collection):
        self.logging_db = logging_db
        self.logging_collection = logging_collection
        self.logging = App_Logger()



    def dropUnnecessaryColumns(self, data):
        '''
                    Method Name: dropUnnecessaryColumns
                    Description: It drops Item_Type column
                    Output: A Dataframe without  Item_Type column
                    On Failure: Raise Exception

                    Written by: Amit Ranjan Sahoo
                    Version: 1.0
                    Revision: None
                '''
        try:
            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Started to drop  Unnecessary Columns')
            data = data.drop(columns=['Item_Type'])
            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Dropped Unnecessary Columns Successfully!!')
            return data

        except Exception as e:
            self.logging.log(self.logging_db, self.logging_collection, 'ERROR', f"Error occured to drop columns: {e}")
            raise e



    def editDataset(self, data):

        '''
                            Method Name: editDataset
                            Description: Editing Item_Visibility, Item_Type, Item_Fat_Content, Item_Identifier, Outlet_Age columns
                            Output: A Dataframe
                            On Failure: Raise Exception

                            Written by: Amit Ranjan Sahoo
                            Version: 1.0
                            Revision: None
                        '''
        try:
            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Started to edit dataset')

            data['Item_Visibility'].replace(0, np.nan, inplace=True) # Replacing 0 Item_Visibility by nan

            #Fixing Item_Fat_Content column
            data['Item_Fat_Content'].replace('low fat', 'Low Fat', inplace=True)
            data['Item_Fat_Content'].replace('LF', 'Low Fat', inplace=True)
            data['Item_Fat_Content'].replace('reg', 'Low Fat', inplace=True)

            #Changing Item_Identifier values by FD, DR, NC
            data['Item_Identifier'] = data['Item_Identifier'].apply(lambda x: x[:2])

            #Extracting the age of the outlets
            data['Outlet_Age'] = 2013 - data['Outlet_Establishment_Year']
            data = data.drop(columns=['Outlet_Establishment_Year'])

            #Chaning the Item_Fat_Content values having Item_Identifier = 'NC', by 'Non Edible'
            data.loc[data['Item_Identifier'] == "NC", 'Item_Fat_Content'] = 'Non Edible'

            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Dataset edited Successfully!!')
            return data

        except Exception as e:
            self.logging.log(self.logging_db, self.logging_collection, 'ERROR', f"Error occured to edit dataset: {e}")
            raise e


    def encodeCategoricalValues(self, data):
        '''
                            Method Name: encodeCategoricalValuesClassification
                            Description: It encodes categorical values
                            Output: A Dataframe with encoded categorical values
                            On Failure: Raise Exception

                            Written by: Amit Ranjan Sahoo
                            Version: 1.0
                            Revision: None
                        '''

        try:
            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Started to encode Categorical Values')
            data['Outlet_Size'] = data['Outlet_Size'].map({'Small': 0, 'Medium': 1, 'High': 2})

            onehot_col = ['Item_Identifier', 'Item_Fat_Content', 'Outlet_Identifier', 'Outlet_Type',
                          'Outlet_Location_Type']
            onehot_enc = pickle.load(open('Encoding/encoder.pickle', 'rb'))
            enc_array = onehot_enc.transform(data[onehot_col])
            enc_df = pd.DataFrame(enc_array, columns=onehot_enc.get_feature_names_out())
            data = data.drop(columns=onehot_col)
            data = pd.concat([data, enc_df], axis=1)
            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Encoded Categorical Values Successfully!!')

            return data

        except Exception as e:
            self.logging.log(self.logging_db, self.logging_collection, 'ERROR', f"Error occured to encode Categorical Values: {e}")

            raise e



    def imputeMissingValues(self, data):
        '''
                                    Method Name: imputeMissingValues
                                    Description: It imputes missing values
                                    Output: A Dataframe with imputed misssing values
                                    On Failure: Raise Exception

                                    Written by: Amit Ranjan Sahoo
                                    Version: 1.0
                                    Revision: None
                                '''

        try:
            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Started to impute Missing Values')

            imputer = KNNImputer(n_neighbors=3, weights='uniform', missing_values=np.nan)
            new_array = imputer.fit_transform(data)  # impute the missing values
            data = pd.DataFrame(data=new_array, columns=data.columns)
            data['Outlet_Size'] = np.round(data['Outlet_Size'])

            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Imputed Missing Values Successfully!!')
            return data

        except Exception as e:
            self.logging.log(self.logging_db, self.logging_collection, 'ERROR', f"Error occured to impute Missing Values: {e}")

            raise e



    def scaleNumericalValues(self, data):
        '''
                            Method Name: scaleNumericalValuesClassification
                            Description: It scales numerical values
                            Output: A Dataframe with scaled numerical values
                            On Failure: Raise Exception

                            Written by: Amit Ranjan Sahoo
                            Version: 1.0
                            Revision: None
                '''

        try:
            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Started to encode Numerical Values')

            num_cols = ['Item_Weight', 'Item_Visibility', 'Item_MRP', 'Outlet_Age']
            num_df = data[num_cols]
            cat_df = data.drop(columns=num_cols)
            scaler = pickle.load(open('Scaling/scaler.pickle', 'rb'))
            num_array = scaler.transform(num_df)
            num_df = pd.DataFrame(num_array, columns=num_df.columns)
            data = pd.concat([num_df, cat_df], axis=1)

            self.logging.log(self.logging_db, self.logging_collection, 'INFO', 'Scaled Numerical Values Successfully!!')
            return data

        except Exception as e:
            self.logging.log(self.logging_db, self.logging_collection, 'ERROR', f"Error occured to Scale Numerical Values: {e}")
            raise e


