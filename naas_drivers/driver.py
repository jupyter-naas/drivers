import pandas as pd

basic_text = "Not defined, it should to allow user to connect"
basic_error = "Not defined, it should return a Dataframe"
connect_error = "You should connect first"


class ConnectDriver:

    connected = False
    raise_error = False

    def raise_for_error(self, raise_error=True):
        self.raise_error = raise_error

    def connect(self, *args, **kwargs):
        if self.raise_error:
            raise ValueError(basic_text)
        else:
            print(basic_text, *args, **kwargs)
        self.connected = True
        return self

    def check_connect(self):
        if not self.connected:
            if self.raise_error:
                raise ValueError(connect_error)
            else:
                print(connect_error)
                return


class InDriver(ConnectDriver):
    def convert_data_to_df(self, *args, **kwargs):
        return basic_error

    def get(self, *args, **kwargs) -> pd.DataFrame:
        self.check_connect()
        if self.raise_error:
            raise ValueError(basic_error)
        else:
            print(basic_text, *args, **kwargs)
            return


class OutDriver(ConnectDriver):
    def send(self, *args, **kwargs):
        self.check_connect()
        if self.raise_error:
            raise ValueError(basic_error)
        else:
            print(basic_text, *args, **kwargs)
        return basic_error
