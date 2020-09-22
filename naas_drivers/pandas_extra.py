import pandas as pd


class PandasExtra:
    def dict_to_df(self, dict_arr):
        df = pd.DataFrame.from_dict(dict_arr)
        return df

    def list_to_df(self, list_arr):
        return pd.DataFrame.from_records([s for s in list_arr])

    def help(self):
        print(f"=== {type(self).__name__} === \n")
        print(".dict_to_df(dict_arr) => convert dict in df\n")
        print(".list_to_df(list_arr) => convert list in df\n")
