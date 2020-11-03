from naas_drivers.driver import InDriver, OutDriver
from airtable import Airtable as at
import pandas as pd


class Airtable(InDriver, OutDriver):
    _airtable = None
    _key = None
    _table = None

    def connect(self, key, table):
        self._key = key
        self._table = table
        self._airtable = at(key, table)
        self.connected = True
        return self

    def convert_data_to_df(self, data) -> pd.DataFrame:
        rows = []
        for dat in data:
            rows.append(
                {"id": dat["id"], "createdTime": dat["createdTime"], **dat["fields"]}
            )
        df = pd.DataFrame.from_records(rows)
        df = df.set_index("id")
        return df

    def get(self, **kwagrs):
        self.check_connect()
        data = self._airtable.get_all(**kwagrs)
        return self.convert_data_to_df(data)

    def send(self, **kwagrs):
        self.check_connect()
        return self._airtable.insert(**kwagrs)

    def search(self, **kwagrs):
        self.check_connect()
        data = self._airtable.search(**kwagrs)
        return self.convert_data_to_df(data)

    def update_by_field(self, **kwagrs):
        self.check_connect()
        return self._airtable.update_by_field(**kwagrs)

    def delete_by_field(self, **kwagrs):
        self.check_connect()
        return self._airtable.delete_by_field(**kwagrs)
