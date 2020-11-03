from naas_drivers.driver import InDriver
import pandas as pd
import datetime as dt


class Yahoofinance(InDriver):
    def get(
        self,
        stock_companies,
        date_from=-36,
        date_to="today",
        interval="1d",
        moving_averages=[],
    ):
        """ generate financial data """
        if isinstance(stock_companies, str):
            stock_companies = [stock_companies]
        if isinstance(date_from, int) and date_from < 0:
            date_from = dt.datetime.today() + dt.timedelta(days=date_from)
        else:
            raise ValueError(f"date_from ({date_from}) cannot be positive")
        if isinstance(date_to, int) and date_to > 0:
            date_to = dt.datetime.today() + dt.timedelta(days=date_to)
        if isinstance(date_to, str) and date_to == "today":
            date_to = dt.datetime.today()
        else:
            raise ValueError(f"date_to ({date_to}) cannot be negative")
        stocks = None
        period1 = date_from.strftime("%s")
        period2 = date_to.strftime("%s")
        for company in stock_companies:
            url = (
                f"https://query1.finance.yahoo.com/v7/finance/download/"
                f"{company}?period1={period1}&period2={period2}&interval={interval}&events=history"
            )
            stock = pd.read_csv(url)
            stock["Company"] = company
            for moving_average in moving_averages:
                stock[f"MA{moving_average}"] = stock.Close.rolling(
                    moving_average
                ).mean()
            if stocks is None:
                stocks = stock
            else:
                stocks = stocks.append(stock)
            print("getted data for", company, url)
        stocks["Date"] = pd.to_datetime(stocks["Date"], format="%Y-%m-%d")
        return stocks
