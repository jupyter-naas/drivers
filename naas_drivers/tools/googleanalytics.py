"""Google Analytics Driver."""
import os
from typing import List

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
    def _get_body(view_id: str,
                  date_ranges: List[dict],
                  metrics: List[dict],
                  pivots_dimensions: List[dict],
                  dimensions: List[dict]=[{'name': 'ga:yearMonth'}]) -> dict:
        """
        Create the body of the request to Google Analytics Reporting API V4.

        Args:
            view_id: your access point for reports; a defined view of data from a property.
            date_ranges: e.g. [{"startDates": "2020-01-01", "endDates": "2020-12-31"}]
            metrics: e.g. [{'expression': 'ga:users'}, {"expression": "ga:bounceRate"}]
            pivot_dimension: e.g. [{"name": "ga:channelGrouping"}]
            dimensions: e.g. [{'name': 'ga:yearMonth'}]

        Returns response in JSON format.
        """
        return {'reportRequests': [{'viewId': view_id, 
                            'dateRanges': date_ranges,
                            'metrics': metrics,
                            'dimensions': dimensions,
                            "pivots": [{"dimensions": pivots_dimensions,
                                        "metrics": metrics
                                       }]
                          }]}

    def get_unique_visitors(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get the number of unique visitors.
        """
        # Setup Request Parameters
        date_ranges = {"startDate": start_date, "endDate": end_date}
        metrics = [{"expression": "ga:users"}]
        pivots_dimensions = [{"name": "ga:channelGrouping"}]
        dimensions = [{"name": "ga:yearMonth"}]
        # Create body
        body = self._get_body(self.view_id, date_ranges, metrics, pivots_dimensions, dimensions)
        # Fetch Data
        response = self.service.reports().batchGet(body=body).execute()
        # Format Output
        unique_visitors = self.format_summary(response)
        unique_visitors.reset_index(inplace=True)
        unique_visitors.rename(
            columns={"ga:yearMonth": "year_month", "ga:users": "unique_visitors"}, inplace=True)
        return unique_visitors

    def get_bounce_rate(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get the number of unique visitors.
        """
        # Setup Request Parameters
        date_ranges = {"startDate": start_date, "endDate": end_date}
        metrics = {"expression": "ga:bounceRate"}
        pivots_dimensions = {"name": "ga:channelGrouping"}
        dimensions = {"name": "ga:yearMonth"}
        # Create body
        body = self._get_body(self.view_id, date_ranges, metrics, pivots_dimensions, dimensions)
        # Fetch Data
        response = self.service.reports().batchGet(body=body).execute()
        # Format Output
        bounce_rate = self.format_summary(response)
        bounce_rate['ga:bounceRate'] /= 100
        bounce_rate.reset_index(inplace=True)
        bounce_rate.rename(
            columns={"ga:yearMonth": "year_month", "ga:bounceRate": "bounce_rate"}, inplace=True)
        return bounce_rate

    @staticmethod
    def format_summary(response):
        """
        Format summary table.
        """
        row_index_names = response['reports'][0]['columnHeader']['dimensions']
        row_index = [element['dimensions'] for element in response['reports'][0]['data']['rows']]
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
