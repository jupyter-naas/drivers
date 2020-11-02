from airtable import Airtable as at


class Airtable:
    _airtable = None

    def connect(self, api_key, base_key, table_name):
        self._airtable = at(base_key=base_key, table_name=table_name, api_key=api_key)

    def get_all(self, *agrs):
        return self._airtable.get_all(*agrs)

    def insert(self, *agrs):
        return self._airtable.insert(*agrs)

    def search(self, *agrs):
        return self._airtable.search(*agrs)

    def update_by_field(self, *agrs):
        return self._airtable.update_by_field(*agrs)

    def delete_by_field(self, *agrs):
        return self._airtable.delete_by_field(*agrs)
