import pandas as pd

basic_text = "Define it"
basic_error = "Define it, it should return a Dataframe"


class ConnectDriver:

    connected = False

    def connect(self, *args, **kwargs):
        print(basic_text, *args, **kwargs)
        self.connected = True
        return self

    def check_connect(self):
        if not self.connected:
            raise ValueError("you should call connect first")


class InDriver(ConnectDriver):
    def convert_data_to_df(self, *args, **kwargs):
        return basic_error

    def get(self, *args, **kwargs) -> pd.DataFrame:
        self.check_connect()
        print(basic_text, *args, **kwargs)
        return basic_error


class OutDriver(ConnectDriver):
    def send(self, *args, **kwargs):
        self.check_connect()
        print(basic_text, *args, **kwargs)
        return basic_error
