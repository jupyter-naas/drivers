import plotly.graph_objects as go
import plotly.io as pio
import plotly.express as px
import pandas as pd
import datetime as dt

class Plot:
    """ Plot generator lib"""
    def updateChartCss(self, chart_filename, css_filename):
        """ update css to alreaady generated chart html (chart_filename, css_filename)"""
        html_map = None
        css = None
        with open(chart_filename) as f:
            html_map = f.read()
        with open(css_filename) as f:
            css = f.read()
        if (html_map.find('id="cs_css"') != -1):
            print("to do")
        else:
            result = html_map.replace("<body>",f'<body><style id="cs_css">{css}</style>')
            with open(chart_filename, "w") as f:
                f.write(result)
                f.close()
                
    def financial_candlestick(self, stock_companies, start=(dt.datetime.today() - dt.timedelta(days=365)), end=dt.datetime.today(), interval='1d', filter=True, filter_title='Stock'):
        """ generate financial_candlestick html """
        stocks = []
        data = []
        period1 = start.strftime('%s')
        period2 = end.strftime('%s')
        buttons = []
        buttons.append(dict(
                args=[{'visible': [True for x in stock_companies]}],
                label='All',
                method="restyle",
                ))
        for company in stock_companies:
            print('getting data for', company)
            stock = pd.read_csv(f'https://query1.finance.yahoo.com/v7/finance/download/{company}?period1={period1}&period2={period2}&interval={interval}&events=history')
            stock["Company"] = company
            stocks.append(stock)
            visibility = [x == company for x in stock_companies]
            data.append(go.Candlestick(name=company,
                                        x=stock["Date"],
                                        open=stock["Open"],
                                        high=stock["High"],
                                        low=stock["Low"],
                                        close=stock["Close"]))
            buttons.append(dict(
                            args=[{'visible': visibility}],
                            label=company,
                            visible=True,
                            method="restyle",
                          ))
        print("generating plot")
        updatemenus=list([
                dict(
                    active=0,
                    buttons=list(buttons),
                    direction="down",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.1,
                    xanchor="left",
                    y=1.2,
                    yanchor="top"
                ),
        ])
        layout = None
        if filter:
            layout = dict(xaxis_rangeslider_visible=False, showlegend=False,
                  updatemenus=updatemenus)
        else:
            layout = dict(xaxis_rangeslider_visible=False, showlegend=False,
                  updatemenus=[])
        fig = go.Figure(data=list(data), layout=layout)
        if filter:
            fig.update_layout(
                annotations=[
                    dict(text=filter_title, x=0, xref="paper", y=1.16, yref="paper",
                                         align="left", showarrow=False)
            ])
        return fig


    def tablechart(self, header_values, cells_values, header_color = 'rgb(136,233,175)'):
        """ generate table html """

        fig = go.Figure(data=[go.Table(
            header=dict(values=list(header_values),
                        fill_color=header_color,
                        line_color='white',
                        align='center',
                        font=dict(family="Helvetica", color='white', size=14)),
            cells=dict(values= cells_values,
                    fill_color='white',
                    line_color='lightgray',
                    align='left',
                    font=dict(family="Helvetica", size=12)))
        ])

        fig.update_layout(
            margin=dict(l=10, r=10, t=0, b=0)
        )

        return fig 