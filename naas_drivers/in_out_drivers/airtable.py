from airtable import Airtable as at
import pandas as pd


class Airtable:
    _airtable = None

    def connect(self, api_key, base_key, table_name):
        self._airtable = at(base_key=base_key, table_name=table_name, api_key=api_key)
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
        data = self._airtable.get_all(**kwagrs)
        return self.convert_data_to_df(data)

    def send(self, **kwagrs):
        return self._airtable.insert(**kwagrs)

    def search(self, **kwagrs):
        data = self._airtable.search(**kwagrs)
        return self.convert_data_to_df(data)

    def update_by_field(self, **kwagrs):
        return self._airtable.update_by_field(**kwagrs)

    def delete_by_field(self, **kwagrs):
        return self._airtable.delete_by_field(**kwagrs)
