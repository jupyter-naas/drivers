from naas_drivers.driver import InDriver, OutDriver
from pymongo import MongoClient
import pandas as pd  # noqa: F401
import sys
import time

filter_system = {"name": {"$regex": r"^(?!system\.)"}}


class Mongo(InDriver, OutDriver):
    """Mongo lib"""

    __client = None
    # CONNECT TO MONGODB
    # - Function name : mongo_connect
    # - Arguments : host, port, username, password
    # - Value return : MongoClient
    # - Message :
    # --- Success => "Successfully connected to MongoDB"
    # --- Failed => "Error connecting to MongoDB. Please check configuration"

    def get_client(self):
        return self.__client

    def connect(
        self, mongo_host, mongo_port=None, mongo_username=None, mongo_password=None
    ):
        if mongo_port and mongo_username and mongo_password:
            self.__client = MongoClient(
                mongo_host,
                mongo_port,
                username=mongo_username,
                password=mongo_password,
            )
        else:
            self.__client = MongoClient(mongo_host)

        self.__client.server_info()
        print("Successfully connected to MongoDB")
        self.connected = True
        return self

    # SAVE DF IN MONGODB
    # - Function name : save_df
    # - Arguments : df, collection_name, db_name, cancel & replace => default = False)
    # - Value return : None
    # - Message :
    # --- Success => "Dataframe successfully save in MongoDB"
    # --- Failed => "Failed to save in MongoDB. Please ask Bob for help"

    def send(self, df, collection_name, db_name, replace=False):
        self.check_connect()
        start_time = time.time()
        # Init collection
        mongo_db = self.__client[db_name]
        df_collection = mongo_db[collection_name]

        try:
            # Delete collection if already exist
            if replace:
                for collection in mongo_db.list_collection_names(filter=filter_system):
                    if collection == collection_name:
                        df_collection.drop()

            # Check size => monogdb will only allow 16MB data at a time to be inserted
            chunk_size = round(sys.getsizeof(df) / 16793600)
            if chunk_size > 0:
                my_list = df.to_dict("records")
                lenght = len(my_list)
                ran = list(range(lenght))
                steps = ran[chunk_size::chunk_size]
                steps.extend([lenght])

                # Inser chunks of the dataframe
                i = 0
                for j in steps:
                    df_collection.insert_many(my_list[i:j])
                    i = j
            else:
                df_collection.insert_many(df.to_dict(orient="records"))
            print(
                f"Dataframe {collection_name} successfully save in database {db_name} in MongoDB. Time: --- %s secnds ---"
                % (time.time() - start_time)
            )
        except Exception as e:
            print("Failed to save in MongoDB.")
            print(e.__doc__)
            print(str(e))

    # Get DF IN MONGODB
    # - Function name : df_to_mongo
    # - Arguments : collection_name, db_name, filters => default = {}
    # - Value return : df
    # - Message :
    # --- Success => "Dataframe successfully save in MongoDB"
    # --- Failed => "Failed to save in MongoDB. Please ask Bob for help"

    def get(self, collection_name, db_name, filters={}):
        self.check_connect()
        # Init collection
        mongo_db = self.__client[db_name]
        df_collection = mongo_db[collection_name]

        try:
            # Read
            df = pd.DataFrame(list(df_collection.find(filters)))
            return df
        except Exception as e:
            print("Failed to read MongoDB")
            print(e.__doc__)
            print(str(e))
            return None
