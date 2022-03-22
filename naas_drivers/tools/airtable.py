from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
from airtable import Airtable as at


class Airtable(InDriver, OutDriver):
    _airtable = None
    _key = None
    _table = None

    def connect(self, key, database, table):
        self._key = key
        self._table = table
        self._airtable = at(base_key=database, table_name=table, api_key=key)
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

    def send(self, data):
        self.check_connect()
        data_formated = data
        if isinstance(data, pd.DataFrame):
            for column in data:
                if data[column].dtypes == "datetime64[ns]":
                    data[column] = data[column].astype(str)
            data_formated = data.to_dict(orient="records")
        if isinstance(data_formated, list):
            return self._airtable.batch_insert(data)
        else:
            return self._airtable.insert(data_formated)

    def search(
        self,
        field_name,
        field,
    ):
        self.check_connect()
        data = self._airtable.search(
            field_name,
            field,
        )
        return self.convert_data_to_df(data)

    def update_by_field(self, field_name, field, data):
        self.check_connect()
        return self._airtable.update_by_field(field_name, field, data)

    def delete_by_field(
        self,
        field_name,
        field,
    ):
        self.check_connect()
        return self._airtable.delete_by_field(field_name, field)
