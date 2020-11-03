import pandas as pd


class Connect_driver:

    connected = False

    def connect(self, *args, **kwargs):
        print("define it", *args, **kwargs)
        self.connected = True
        return self

    def check_connect(self):
        if not self.connected:
            raise ValueError("you should call connect first")


class In_driver(Connect_driver):
    def convert_data_to_df(self, *args, **kwargs):
        return "Define it, it should return a Dataframe"

    def get(self, *args, **kwargs) -> pd.DataFrame:
        self.check_connect()
        print("define it", *args, **kwargs)
        return "Define it, it should return a Dataframe"


class Out_driver(Connect_driver):
    def send(self, *args, **kwargs):
        self.check_connect()
        print("define it", *args, **kwargs)
        return "Define it, it should return a Dataframe"
