from naas_drivers.driver import InDriver
from naas_drivers import emailbuilder
import pandas as pd
import requests
import os
from datetime import datetime, timedelta
import plotly.graph_objects as go

DATE_FORMAT = "%Y-%m-%d"
NUMBER_FORMAT = "{:,.2f} €"

QONTO_CARD = "https://lib.umso.co/lib_sluGpRGQOLtkyEpz/98a28jf4yswpuert.png"
NAAS_WEBSITE = "https://www.naas.ai"


class Organizations:
    def __init__(self, user_id, api_key):
        self.base_url = os.environ.get(
            "QONTO_API_URL", "https://thirdparty.qonto.eu/v2"
        )
        self.req_headers = {"authorization": f"{user_id}:{api_key}"}
        self.url = f"{self.base_url}/organizations"
        self.user_id = user_id

    def get(self):
        try:
            req = requests.get(
                url=f"{self.url}/{self.user_id}", headers=self.req_headers
            )
            req.raise_for_status()
            items = req.json()["organization"]["bank_accounts"]
            df = pd.DataFrame.from_records(items)

            # Formating CS
            df["date"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            df = df.drop(["slug", "balance_cents", "authorized_balance_cents"], axis=1)
            df.columns = df.columns.str.upper()
            return df
        except requests.HTTPError as err:
            err_code = err.response.status_code
            err_msg = err.response.json()
            to_print = f"{err_code}: {err_msg}"
            print(to_print)


class Transactions(Organizations):
    def get_all(self):
        # Get organizations
        df_organisations = self.get()

        # For each bank account, get all transactions
        df_transaction = pd.DataFrame()
        for _, row in df_organisations.iterrows():
            iban = row["IBAN"]

            # Get transactions
            current_page = "1"
            has_more = True
            while has_more:
                req = requests.get(
                    url=f"{self.base_url}/transactions?current_page={current_page}?per_page=100&iban={iban}",
                    headers=self.req_headers,
                )
                items = req.json()
                transactions = items["transactions"]
                df = pd.DataFrame.from_records(transactions)
                df["iban"] = iban
                df_transaction = pd.concat([df_transaction, df], axis=0)
                # Check if next page exists
                next_page = items["meta"]["next_page"]
                if next_page is None:
                    has_more = False
                else:
                    current_page = str(next_page)
        # Formatting
        to_keep = [
            "iban",
            "settled_at",
            "emitted_at",
            "transaction_id",
            "label",
            "reference",
            "operation_type",
            "side",
            "amount",
            "currency",
        ]
        df_transaction = (
            df_transaction[to_keep].reset_index(drop=True).fillna("Not affected")
        )
        df_transaction.loc[
            df_transaction["side"] == "debit", "amount"
        ] = df_transaction["amount"] * (-1)
        df_transaction.columns = df_transaction.columns.str.upper()
        return df_transaction


class Statements(Transactions):
    def __get_dates(self, date_from=None, date_to=None):
        dates = []
        # Dates
        if date_from is not None and date_to is None:
            date_to = tmp_df["DATE"].max()
        if date_to is not None and date_from is None:
            date_from = tmp_df["DATE"].min()
        if (date_from and date_to) is not None:
            dates_range = pd.date_range(start=date_from, end=date_to)
            for date in dates_range:
                date = str(date.strftime(DATE_FORMAT))
                dates.append(date)
        return dates

    def __filter_dates(self, df, date_from=None, date_to=None):
        df_filter = pd.DataFrame()
        if "IBAN" in df.columns:
            ibans = df["IBAN"].drop_duplicates().tolist()
            for iban in ibans:
                tmp_df = df[df.IBAN == iban]
                dates = self.__get_dates(date_from, date_to)
                tmp_df = tmp_df[tmp_df["DATE"].isin(dates)]
                df_filter = pd.concat([df_filter, tmp_df], axis=0)
        else:
            dates = self.__get_dates(date_from, date_to)
            df_filter = df[df["DATE"].isin(dates)]
        return df_filter

    def detailed(self, date_from=None, date_to=None):
        df = self.get_all()
        df = df.rename(columns={"EMITTED_AT": "DATE"})
        df["DATE"] = pd.to_datetime(
            df["DATE"], format="%Y-%m-%dT%H:%M:%S.%fZ"
        ).dt.strftime(DATE_FORMAT)

        # Calc positions
        to_sort = ["IBAN", "DATE", "TRANSACTION_ID"]
        df = df.sort_values(by=to_sort).reset_index(drop=True)
        to_group = ["IBAN", "TRANSACTION_ID"]
        df["POSITION"] = df.groupby(to_group, as_index=True).agg({"AMOUNT": "cumsum"})
        to_keep = [
            "IBAN",
            "DATE",
            "TRANSACTION_ID",
            "LABEL",
            "REFERENCE",
            "OPERATION_TYPE",
            "AMOUNT",
            "POSITION",
            "CURRENCY",
        ]
        df = df[to_keep]
        return self.__filter_dates(df, date_from, date_to)

    def aggregated(self, date_from=None, date_to=None):
        df = self.get_all()
        df = df.rename(columns={"EMITTED_AT": "DATE"})
        df["DATE"] = pd.to_datetime(
            df["DATE"], format="%Y-%m-%dT%H:%M:%S.%fZ"
        ).dt.strftime(DATE_FORMAT)

        # Aggregation
        to_group = ["IBAN", "DATE", "CURRENCY"]
        df = df.groupby(to_group, as_index=False).agg({"AMOUNT": "sum"})

        # Calc positions
        to_sort = ["IBAN", "DATE"]
        df = df.sort_values(by=to_sort).reset_index(drop=True)
        to_group = ["IBAN"]
        df["POSITION"] = df.groupby(to_group, as_index=True).agg({"AMOUNT": "cumsum"})
        to_keep = [
            "IBAN",
            "DATE",
            "AMOUNT",
            "POSITION",
            "CURRENCY",
        ]
        df = df[to_keep]
        return self.__filter_dates(df, date_from, date_to)

    def consolidated(self, to_conso=[], date_from=None, date_to=None):
        df = self.get_all()
        df = df.rename(columns={"EMITTED_AT": "DATE"})
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.strftime(DATE_FORMAT)

        # Aggregation
        to_group = ["DATE"]
        for x in to_conso:
            to_group.append(x)
        df = df.groupby(to_group, as_index=False).agg({"AMOUNT": "sum"})

        # Calc positions
        df = df.sort_values(by=["DATE"]).reset_index(drop=True)
        df["POSITION"] = df.agg({"AMOUNT": "cumsum"})
        return self.__filter_dates(df, date_from, date_to)

    def summary(self, date_from=None, date_to=None):

        first_text = (
            f"Solde au {datetime.strptime(date_from, DATE_FORMAT).strftime('%d/%m/%Y')}"
        )
        current_text = (
            f"Solde au {datetime.strptime(date_to, DATE_FORMAT).strftime('%d/%m/%Y')}"
        )

        # Init
        cash_summary = pd.DataFrame()

        # Get data
        df_statement = self.consolidated(
            to_conso=["OPERATION_TYPE"], date_from=date_from, date_to=date_to
        )
        # > Get first position
        first_position = round(df_statement["POSITION"].tolist()[0], 2)
        df_first = pd.DataFrame(
            [{"OPERATION_TYPE": first_text, "ORDER": 0, "AMOUNT": first_position}]
        )
        # > Get transactions
        df_transaction = df_statement.copy()
        # Order
        transaction_order = {
            "income": 1,
            "swift_income": 2,
            "card": 3,
            "transfer": 4,
            "qonto_fee": 5,
            "direct_debit": 6,
            "cheque": 7,
            "recall": 8,
        }
        df_transaction["ORDER"] = df_transaction["OPERATION_TYPE"].replace(
            transaction_order
        )
        # Replace value
        transaction_type = {
            "income": " - Encaissements",
            "swift_income": " - SWIFT",
            "card": " - Flux carte bleue",
            "transfer": " - Virements",
            "qonto_fee": " - Frais bancaires",
            "direct_debit": " - Prélèvements",
            "cheque": " - Chèques",
            "recall": " -  Rappel",
        }
        df_transaction["OPERATION_TYPE"] = df_transaction["OPERATION_TYPE"].replace(
            transaction_type
        )
        # Groupby month
        to_group = ["OPERATION_TYPE", "ORDER"]
        to_agg = {"AMOUNT": "sum"}
        df_transaction = df_transaction.groupby(to_group, as_index=False).agg(to_agg)

        # > Get last position
        last_position = round(df_statement["POSITION"].tolist()[-1], 2)
        df_last = pd.DataFrame(
            [{"OPERATION_TYPE": current_text, "ORDER": 99, "AMOUNT": last_position}]
        )

        # Create summary table
        cash_summary = pd.concat([df_first, df_transaction, df_last], axis=0)
        cash_summary = cash_summary.sort_values("ORDER").drop("ORDER", axis=1)
        cash_summary["AMOUNT"] = (
            cash_summary["AMOUNT"].map(NUMBER_FORMAT.format).str.replace(",", " ")
        )  # .str.replace(".", ",")
        cash_summary = cash_summary.rename(
            columns={"OPERATION_TYPE": "Type", "AMOUNT": "Montant"}
        )
        return cash_summary

    def barline(
        self,
        date_from=None,
        date_to=None,
        title=None,
        line_name="Solde",
        line_color="#1ea1f1",
        cashin_name="Encaissements",
        cashin_color="#47dd82",
        cashout_name="Décaissements",
        cashout_color="#ea484f",
    ):
        # Data linechart
        df_line = self.consolidated(date_from=date_from, date_to=date_to)
        df_line = df_line[["DATE", "POSITION"]]
        df_line["DATE"] = pd.to_datetime(df_line["DATE"])

        # Data bar
        df_bar = self.consolidated(
            to_conso=["TRANSACTION_ID", "LABEL", "OPERATION_TYPE"],
            date_from=date_from,
            date_to=date_to,
        )
        df_bar.loc[df_bar.AMOUNT > 0, "FLOWS"] = "CASH_IN"
        df_bar.loc[df_bar.AMOUNT < 0, "FLOWS"] = "CASH_OUT"
        df_bar["YEAR"] = pd.to_datetime(df_bar["DATE"]).dt.strftime("%Y")
        df_bar["MONTH"] = pd.to_datetime(df_bar["DATE"]).dt.strftime("%m")
        df_bar["DATE"] = df_bar["YEAR"] + "-" + df_bar["MONTH"]
        df_bar = df_bar.groupby(["DATE", "FLOWS"], as_index=False).agg(
            {"AMOUNT": "sum"}
        )
        cashin = df_bar[df_bar["FLOWS"] == "CASH_IN"]
        cashout = df_bar[df_bar["FLOWS"] == "CASH_OUT"]

        # Set up title
        if title is None:
            date_from = datetime.strptime(date_from, DATE_FORMAT).strftime("%d/%m/%Y")
            date_to = datetime.strptime(date_to, DATE_FORMAT).strftime("%d/%m/%Y")
            title = f"Evolution du {date_from} au {date_to}"
        # Init fig
        fig = go.Figure()

        # Create chart
        fig.add_trace(
            go.Scatter(
                x=df_line["DATE"],
                y=df_line["POSITION"],
                name=line_name,
                marker=dict(color=line_color),
            )
        )
        fig.add_bar(
            x=cashin["DATE"],
            y=cashin["AMOUNT"],
            name=cashin_name,
            marker=dict(color=cashin_color),
        )
        fig.add_bar(
            x=cashout["DATE"],
            y=cashout["AMOUNT"],
            name=cashout_name,
            marker=dict(color=cashout_color),
        )

        # Update chart display
        fig.update_layout(plot_bgcolor="#ffffff", hovermode="x", title=title)
        fig.update_yaxes(tickprefix="€", gridcolor="#eaeaea")
        fig.update_xaxes(dtick="M1")
        return fig

    def email(
        self,
        date_from,
        date_to,
        last_position,
        graph_img,
        graph_link,
        cash_summary,
        nb_last,
        df_last,
        statement_link,
    ):

        # Format variable
        date_from = datetime.strptime(date_from, DATE_FORMAT).strftime("%d/%m/%Y")
        date_to = datetime.strptime(date_to, DATE_FORMAT).strftime("%d/%m/%Y")
        last_position = NUMBER_FORMAT.format(last_position).replace(
            ",", " "
        )  # .replace(".", ",")

        # Table style
        table_style = {
            "border": True,
            "header": True,
            "header_bg_color": "#6b5aed",
            "header_ft_color": "white",
            #             'border_color': '#BFBFBF',
        }

        table_cash = {
            "col_size": {
                0: "80%",
                1: "20%",
            },
            "col_align": {
                0: "left",
                1: "right",
            },
        }

        table_last_week = {
            "col_size": {0: "40%", 1: "20%", 2: "20%", 3: "20%"},
            "col_align": {
                0: "left",
                1: "center",
                2: "center",
                3: "right",
            },
        }

        # Email content
        content = {
            "title": (
                f"<a href='{NAAS_WEBSITE}'>"
                f"<img align='center' width='100%' target='_blank' style='border-radius:5px;'"
                f"src='{QONTO_CARD}'>"
                "</a>"
            ),
            "txt_intro": (
                f"Hello,<br><br>"
                f"Vous disposez de <b>{last_position}</b> chez Qonto au {date_to}."
            ),
            "title_1": emailbuilder.heading(
                "Evolution de votre trésorerie", underline=True
            ),
            "txt_1_1": emailbuilder.text(
                f"Voici un graphique de l’évolution des encaissements/décaissements depuis le {date_from}."
            ),
            "cash_graphic": emailbuilder.image(graph_img, link=graph_link),
            "txt_1_2": emailbuilder.text(
                "Cliquez sur l'image pour accéder au graphique dynamique.",
                font_size="14px",
                text_align="center",
                italic=True,
            ),
            "title_2": emailbuilder.heading(
                "Tableaux de flux par type", underline=True
            ),
            "cash_table": emailbuilder.table(
                cash_summary, **{**table_style, **table_cash}
            ),
            "title_3": emailbuilder.heading("Dernières transactions", underline=True),
            "txt_3_1": emailbuilder.text(
                f"Voici la liste des transactions effectuées ces {abs(nb_last)} derniers jours:"
            ),
            "week_table": emailbuilder.table(
                df_last, **{**table_style, **table_last_week}
            ),
            "txt_end_1": emailbuilder.text("Pour accéder au détail de vos flux:"),
            "button": emailbuilder.button(
                statement_link, "Télécharger le fichier Excel"
            ),
            "txt_end_2": emailbuilder.text(
                "Pour aller plus loin dans votre analyse découvrez tous "
                "<a href='https://github.com/jupyter-naas/awesome-notebooks/tree/master/Qonto'>les templates Qonto disponible sur Naas</a>"
                " ou contactez-nous sur hello@naas.ai pour personnaliser cet email."
            ),
            "footer_cs": emailbuilder.footer_company(naas=True),
        }
        # Generate email in html
        email_content = emailbuilder.generate(display='iframe', **content)
        return email_content

    def transactions(self, date_from=None, date_to=None):
        # Data
        df = self.consolidated(
            to_conso=["LABEL", "OPERATION_TYPE"], date_from=date_from, date_to=date_to
        )

        # Calc week ago
        if type(date_from) is int and date_from < 0:
            week_ago = (
                datetime.strptime(date_to, DATE_FORMAT) + timedelta(days=date_from)
            ).strftime(DATE_FORMAT)
        # Filter data
        df = df[(df["DATE"] > week_ago) & (df["DATE"] < date_to)]

        # Groupby
        to_group = ["LABEL", "OPERATION_TYPE", "DATE"]
        df = df.groupby(to_group, as_index=False).agg({"AMOUNT": "sum"})
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.strftime("%d/%m/%Y")
        # Replace value
        transaction_type = {
            "income": "Encaissement",
            "swift_income": "SWIFT",
            "card": "Carte bleue",
            "transfer": "Virement",
            "qonto_fee": "Frais bancaires",
            "direct_debit": "Prélèvement",
            "cheque": "Chèque",
            "recall": "Rappel",
        }
        df["OPERATION_TYPE"] = df["OPERATION_TYPE"].replace(transaction_type)

        # Rename columns
        week_cols = {
            "LABEL": "Description",
            "OPERATION_TYPE": "Type",
            "DATE": "Date",
            "AMOUNT": "Montant",
        }
        df = df.rename(columns=week_cols)
        df["Montant"] = (
            df["Montant"].map("{:,.2f} €".format).str.replace(",", " ")
        )  # .str.replace(".", ",")
        df = df.sort_values("Date", ascending=False)
        return df


class Qonto(InDriver):
    user_id = None
    api_token = None

    def connect(self, user_id, api_token):
        # Init thinkific attribute
        self.user_id = user_id
        self.token = api_token

        # Init end point
        self.positions = Organizations(self.user_id, self.token)
        self.flows = Transactions(self.user_id, self.token)
        self.statement = Statements(self.user_id, self.token)

        # Set connexion to active
        self.connected = True
        return self
