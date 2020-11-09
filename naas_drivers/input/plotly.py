from naas_drivers.driver import InDriver
import plotly.graph_objects as go
import requests
import os


class Plotly(InDriver):
    """ Plot generator lib"""

    __css_base = ".modebar {display: none;} \n.modebar-container {display: none;} "

    def __export(self, chart, filename, css=None):
        html_filename = f"{filename.split('.')[0]}.html"
        html_exist = os.path.exists(html_filename)
        chart.write_html(html_filename)
        html_map = None
        if css is None:
            css = self.__css_base
        else:
            css = css + self.__css_base
        with open(html_filename) as f:
            html_map = f.read()
            result = html_map.replace(
                "</head>", f'<style id="naas_css">{css}</style></head>'
            )
        with open(html_filename, "w") as f:
            f.write(result)
            f.close()
        if filename.endswith(".png") or filename.endswith(".jpeg"):
            html_text = result
            extension = filename.split(".")[1]
            json = {
                "output": "screenshot",
                "html": html_text,
                "emulateScreenMedia": True,
                "ignoreHttpsErrors": True,
                "scrollPage": False,
                "screenshot": {"type": extension, "selector": ".cartesianlayer"},
            }
            req = requests.post(
                url=f"{os.environ.get('SCREENSHOT_API', 'http://naas-screenshot:9000')}/api/render",
                json=json,
            )
            req.raise_for_status()
            open(filename, "wb").write(req.content)
            if not html_exist:
                os.remove(html_filename)
        elif not filename.endswith(".html"):
            print("Not supported for now")
            os.remove(html_filename)
            return
        print(f"Saved as {filename}")

    def export(self, chart, filenames, css=None):
        """ create html export and add css to it"""
        if isinstance(filenames, list):
            for filename in filenames:
                self.__export(chart, filename, css)
        else:
            self.__export(chart, filenames, css)

    def __predict(self, stock, visible):
        charts = []
        if "ARIMA" in stock.columns:
            charts.append(
                go.Scatter(
                    visible=visible,
                    x=stock["Date"],
                    y=stock["ARIMA"],
                    mode="lines",
                    name="ARIMA",
                    line=dict(dash="dot"),
                )
            )
        if "COMPOUND" in stock.columns:
            charts.append(
                go.Scatter(
                    visible=visible,
                    x=stock["Date"],
                    y=stock["COMPOUND"],
                    mode="lines",
                    name="COMPOUND",
                    line=dict(dash="dot"),
                )
            )
        if "LINEAR" in stock.columns:
            charts.append(
                go.Scatter(
                    visible=visible,
                    x=stock["Date"],
                    y=stock["LINEAR"],
                    mode="lines",
                    name="LINEAR",
                    line=dict(dash="dot"),
                )
            )
        if "SVR" in stock.columns:
            charts.append(
                go.Scatter(
                    visible=visible,
                    x=stock["Date"],
                    y=stock["SVR"],
                    mode="lines",
                    name="SVR",
                    line=dict(dash="dot"),
                )
            )
        return charts

    def __linechart(self, stock, visible, kind):
        charts = []
        filtered = kind.split("_")[1] if "_" in kind else None
        if "Open" in stock.columns and (filtered is None or filtered == "open"):
            charts.append(
                go.Scatter(
                    visible=visible,
                    x=stock["Date"],
                    y=stock["Open"],
                    mode="lines",
                    name="Open",
                )
            )
        if "Close" in stock.columns and (filtered is None or filtered == "close"):
            charts.append(
                go.Scatter(
                    visible=visible,
                    x=stock["Date"],
                    y=stock["Close"],
                    mode="lines",
                    name="Close",
                )
            )
        return charts

    def __candlestick(self, stock, visible, company):
        return [
            go.Candlestick(
                visible=visible,
                name=company,
                x=stock["Date"],
                open=stock["Open"],
                high=stock["High"],
                low=stock["Low"],
                close=stock["Close"],
            )
        ]

    def __moving_average(self, stock, visible):
        filter_cols = [x for x in stock.columns if x.startswith("MA")]
        if len(filter_cols) == 0:
            return []
        else:
            charts = []
            for i in range(len(filter_cols)):
                filter_col = filter_cols[i]
                line = dict(color="green", width=1) if filter_col == "MA20" else None
                line = dict(color="red", width=1) if filter_col == "MA50" else line
                charts.append(
                    go.Scatter(
                        x=stock["Date"],
                        visible=visible,
                        y=stock[filter_col],
                        line=line,
                        name=f'{filter_col.replace("MA", "")} MA',
                    )
                )
            return charts

    def linechart(
        self,
        dataset,
        label_x: str,
        label_y: list,
        show=False,
    ):
        charts = []
        layout = dict(
            dragmode="pan",
            xaxis_rangeslider_visible=False,
            showlegend=False,
            updatemenus=[],
        )
        for i in range(len(label_y)):
            filter_col = label_y[i]
            charts.append(
                go.Scatter(
                    x=dataset[label_x],
                    y=dataset[filter_col],
                    name=filter_col,
                )
            )
        fig = go.Figure(data=charts, layout=layout)
        if show:
            fig.show()
        return fig

    def candlestick(
        self,
        dataset,
        label_x: str,
        label_open: str,
        label_high: str,
        label_low: str,
        label_close: str,
        show=False,
    ):
        charts = []
        layout = dict(
            dragmode="pan",
            xaxis_rangeslider_visible=False,
            showlegend=False,
            updatemenus=[],
        )
        charts = [
            go.Candlestick(
                x=dataset[label_x],
                open=dataset[label_open],
                high=dataset[label_high],
                low=dataset[label_low],
                close=dataset[label_close],
            )
        ]
        fig = go.Figure(data=charts, layout=layout)
        if show:
            fig.show()
        return fig

    def stock(
        self,
        stock_data,
        kind="candlestick",
        show=False,
        filter=False,
        filter_title="Stock",
        filter_all=False,
    ):
        """ generate financial_chart """
        stock_data_copy = stock_data.copy()
        if "Company" not in stock_data_copy:
            stock_data_copy["Company"] = "Company_1"
        stock_companies = stock_data_copy.Company.unique()
        data = []
        buttons = []
        if filter_all:
            buttons.append(
                dict(
                    args=[{"visible": [True for x in stock_companies]}],
                    label="All",
                    method="restyle",
                )
            )
        for y in range(len(stock_companies)):
            company = stock_companies[y]
            stock = stock_data_copy.loc[stock_data_copy["Company"] == company]
            charts = []
            visible = filter_all if filter_all else y == 0
            charts.extend(self.__moving_average(stock, visible))
            if kind == "candlestick":
                charts.extend(self.__candlestick(stock, visible, company))
            elif kind.startswith("linechart"):
                if any(
                    x in ["ARIMA", "LINEAR", "SVR", "COMPOUND"] for x in stock.columns
                ):
                    charts.extend(self.__predict(stock, visible))
                if any(x in ["Open", "Close"] for x in stock.columns):
                    charts.extend(self.__linechart(stock, visible, kind))
            else:
                print("Not supported for now")
                return
            visibility = []
            for x in stock_companies:
                visibility.extend([x == company] * len(charts))
            data.extend(charts)
            buttons.append(
                dict(
                    args=[{"visible": visibility}],
                    label=company,
                    visible=True,
                    method="restyle",
                )
            )
        print(f"Chart {kind} generated")
        updatemenus = list(
            [
                dict(
                    active=0,
                    buttons=list(buttons),
                    direction="down",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.1,
                    xanchor="left",
                    y=1.2,
                    yanchor="top",
                ),
            ]
        )
        layout = None
        if filter:
            layout = dict(
                dragmode="pan",
                xaxis_rangeslider_visible=False,
                showlegend=False,
                updatemenus=updatemenus,
            )
        else:
            layout = dict(
                dragmode="pan",
                xaxis_rangeslider_visible=False,
                showlegend=False,
                updatemenus=[],
            )
        fig = go.Figure(data=list(data), layout=layout)
        if filter:
            fig.update_layout(
                template="plotly_white",
                margin=dict(t=0, b=0, l=0, r=0),
                annotations=[
                    dict(
                        text=filter_title,
                        x=0,
                        xref="paper",
                        font=dict(size=24),
                        y=1.16,
                        yref="paper",
                        align="left",
                        showarrow=False,
                    )
                ],
            )
        if show:
            fig.show()
        return fig

    def table(
        self, header_values, cells_values, header_color="rgb(136,233,175)", show=False
    ):
        """ generate table html """

        fig = go.Figure(
            data=[
                go.Table(
                    header=dict(
                        values=list(header_values),
                        fill_color=header_color,
                        line_color="white",
                        align="center",
                        font=dict(family="Helvetica", color="white", size=14),
                    ),
                    cells=dict(
                        values=cells_values,
                        fill_color="white",
                        line_color="lightgray",
                        align="left",
                        font=dict(family="Helvetica", size=12),
                    ),
                )
            ]
        )

        fig.update_layout(margin=dict(l=10, r=10, t=0, b=0))
        if show:
            fig.show()
        return fig
