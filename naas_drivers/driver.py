import pandas as pd

basic_text = "Not defined, it should to allow user to connect"
key_text = "Connect key missing"
basic_error = "Not defined, it should return a Dataframe"
connect_error = "You should connect first"


class ConnectDriver:

    connected = False
    key = None
    raise_error = False

    def open_or_read(self, data):
        read_data = data
        try:
            read_data = open(data, "r").read()
        except OSError:
            pass
        return read_data

    def print_error(self, error):
        if self.raise_error:
            raise ValueError(error)
        else:
            print(error)

    def raise_for_error(self, raise_error=True):
        self.raise_error = raise_error

    def connect(self, key=None):
        self.key = key
        if self.key:
            self.connected = True
        else:
            self.print_error(key_text)
        return self

    def check_connect(self):
        if not self.connected:
            self.print_error(connect_error)


class InDriver(ConnectDriver):
    def convert_data_to_df(self, *args, **kwargs):
        return basic_error

    def get(self, *args, **kwargs) -> pd.DataFrame:
        self.check_connect()
        self.print_error(basic_error)


class OutDriver(ConnectDriver):
    def send(self, *args, **kwargs):
        self.check_connect()
        self.print_error(basic_error)
        return basic_error
