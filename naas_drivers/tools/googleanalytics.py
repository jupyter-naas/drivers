"""Google Analytics Driver."""
import os

import numpy as np
import pandas as pd
from google.oauth2 import service_account
from apiclient.discovery import build

from naas_drivers.driver import InDriver, OutDriver


class GoogleAnalytics(InDriver, OutDriver):
    """
    Google Analytics driver.
    """

    def __init__(self, view_id: str) -> None:
        self.view_id = view_id

    def connect(self) -> None:
        credentials = service_account.Credentials.from_service_account_file(
            os.getenv("GCP_SERVICE_ACCOUNT_JSON"), 
            scopes = ['https://www.googleapis.com/auth/analytics.readonly'])
        self.service = build('analyticsreporting', 'v4', credentials=credentials)
        return self

    @staticmethod
    def _get_body(view_id: str, start_date: str, end_date: str) -> dict:
        return {'reportRequests': [{'viewId': view_id, 
                            'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                            'metrics': [{'expression': 'ga:users'}, 
                                        {"expression": "ga:bounceRate"}],
                            'dimensions': [{'name': 'ga:yearMonth'}],
                            "pivots": [{"dimensions": [{"name": "ga:channelGrouping"}],
                                        "metrics": [{"expression": "ga:users"},
                                                    {"expression": "ga:bounceRate"}]
                                       }]
                          }]}

    def get_traffic_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get traffic data from Google Analytics.

        Args:
            start_date: First day to consider in the report.
            end_date: Last day to consider in the report.

        Returns a pandas DataFrame with the traffic data.
        """
        body = self._get_body(self.view_id, start_date, end_date)
        response = self.service.reports().batchGet(body=body).execute()
        return self.format_pivot(response)

    @staticmethod
    def format_summary(response):
        """
        Format summary table.
        """
        row_index_names = response['reports'][0]['columnHeader']['dimensions']
        row_index = [ element['dimensions'] for element in response['reports'][0]['data']['rows']]
        row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)), 
                                                    names = np.array(row_index_names))
        # extract column names
        summary_column_names = [item['name'] for item in response['reports'][0]
                                ['columnHeader']['metricHeader']['metricHeaderEntries']]
        # extract table values
        summary_values = [element['metrics'][0]['values']
                          for element in response['reports'][0]['data']['rows']]
        # combine. I used type 'float' because default is object, and as far as I know, all values are numeric
        df = pd.DataFrame(data = np.array(summary_values), 
                        index = row_index_named, 
                        columns = summary_column_names).astype('float')
        return df

    @staticmethod
    def format_pivot(response):
        """
        Creates the final dataframe.
        """
        # extract table values
        pivot_values = [item['metrics'][0]['pivotValueRegions'][0]['values']
                        for item in response['reports'][0]['data']['rows']]
        # create column index
        top_header = [item['dimensionValues'] for item in response['reports'][0]
                    ['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
        column_metrics = [item['metric']['name'] for item in response['reports'][0]
                        ['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
        array = np.concatenate((np.array(top_header),
                                np.array(column_metrics).reshape((len(column_metrics),1))), 
                            axis = 1)
        column_index = pd.MultiIndex.from_arrays(np.transpose(array))
        # create row index
        row_index_names = response['reports'][0]['columnHeader']['dimensions']
        row_index = [ element['dimensions'] for element in response['reports'][0]['data']['rows']]
        row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)), 
                                                    names = np.array(row_index_names))
        # combine into a dataframe
        df = pd.DataFrame(data = np.array(pivot_values), 
                        index = row_index_named, 
                        columns = column_index).astype('float')
        return df

    def format_report(self, response):
        """
        Format final report as a pandas DataFrame.
        """
        summary = self.format_summary(response)
        pivot = self.format_pivot(response)
        if pivot.columns.nlevels == 2:
            summary.columns = [['']*len(summary.columns), summary.columns]
        return(pd.concat([summary, pivot], axis = 1))
