import pandas as pd
import requests
from datetime import datetime
import plotly.graph_objects as go
import urllib
from naas_drivers.tools.emailbuilder import EmailBuilder
import pydash as _pd

emailbuilder = EmailBuilder()

QONTO_API_URL = "https://thirdparty.qonto.com/v2"
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
NUMBER_FORMAT = "{:,.2f} €"

QONTO_CARD = "https://lib.umso.co/lib_sluGpRGQOLtkyEpz/98a28jf4yswpuert.png"
NAAS_WEBSITE = "https://www.naas.ai"


class Qonto:
    @staticmethod
    def get_dates(df, date_column, date_from=None, date_to=None):

        # Get all dates for range
        filter_df = 0
        dates = []
        if date_to is None:
            date_to = df[date_column].max()
        else:
            date_to = datetime.strptime(date_to, DATE_FORMAT)
        if date_from is None:
            date_from = df[date_column].min()
        elif type(date_from) == int:
            filter_df = date_from
            date_from = df[date_column].min()
        else:
            date_from = datetime.strptime(date_from, DATE_FORMAT)
        dates_range = pd.date_range(start=date_from, end=date_to)
        for date in dates_range:
            date = str(date.strftime(DATE_FORMAT))
            dates.append(date)
        return dates[filter_df:]

    @staticmethod
    def filter_dates(df, date_column, date_from=None, date_to=None):

        # Create columns date temp to apply filter
        df["DATE_TMP"] = pd.to_datetime(df[date_column]).dt.strftime(DATE_FORMAT)

        # Get list of dates
        dates = Qonto.get_dates(df, "DATE_TMP", date_from=date_from, date_to=date_to)

        # Filter on new columns
        df = df[df["DATE_TMP"].isin(dates)].reset_index(drop=True)
        return df.drop("DATE_TMP", axis=1)

    def connect(self, user_id, api_token):
        # Init thinkific attribute
        self.user_id = user_id
        self.api_token = api_token

        # Init headers
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"{self.user_id}:{self.api_token}",
        }

        # Init end point
        self.organizations = Organizations(self.user_id, self.headers)
        self.positions = Organizations(self.user_id, self.headers)
        self.transactions = Transactions(self.user_id, self.headers)
        self.statements = Statements(self.user_id, self.headers)

        # Set connexion to active
        self.connected = True
        return self


class Organizations(Qonto):
    def __init__(self, user_id, headers):
        Qonto.__init__(self)
        self.user_id = user_id
        self.headers = headers

    def get(self, cols_to_drop=["SLUG", "BALANCE_CENTS", "AUTHORIZED_BALANCE_CENTS"]):
        """
        Return an dataframe object with 11 columns:
        - IBAN
        - BIC
        - CURRENCY
        - BALANCE
        - AUTHORIZED_BALANCE
        - NAME
        - UPDATED_AT
        - STATUS
        - ORGANIZATION_SLUG
        - LEGAL_NAME
        - EXTRACT_DATE

        Parameters
        ----------
        cols_to_drop: list (default None):
            Columns to drop from your dataframe.
        """

        req_url = f"{QONTO_API_URL}/organization"
        res = requests.get(req_url, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        res_json = res.json()

        # Get bank accounts
        slug = _pd.get(res_json, "organization.slug")
        legal_name = _pd.get(res_json, "organization.legal_name")
        accounts = _pd.get(res_json, "organization.bank_accounts")
        df = pd.DataFrame.from_records(accounts)
        df["organization_slug"] = slug
        df["legal_name"] = legal_name

        # Cleaning naas
        df["extract_date"] = datetime.now().strftime(DATETIME_FORMAT)
        df = df.fillna("Not defined")
        df.columns = df.columns.str.upper()

        # Columns to drop
        for cols in cols_to_drop:
            df = df.drop(cols, axis=1)
        return df


class Transactions(Qonto):
    def __init__(self, user_id, headers):
        Qonto.__init__(self)
        self.user_id = user_id
        self.headers = headers

    def get(self, date="EMITTED_AT", date_from=None, date_to=None):
        """
        Return an dataframe object with 22 columns:
        - IBAN
        - SETTLED_AT
        - EMITTED_AT
        - TRANSACTION_ID
        - TRANSACTION_ORDER
        - LABEL
        - STATUS
        - CATEGORY
        - REFERENCE
        - OPERATION_TYPE
        - CARD_LAST_DIGITS
        - SIDE
        - AMOUNT
        - CURRENCY
        - VAT_AMOUNT
        - VAT_RATE
        - LOCAL_AMOUNT
        - LOCAL_CURRENCY
        - NOTE
        - ATTACHMENT_IDS
        - ATTACHMENT_LOST
        - ATTACHMENT_REQUIRED

        Parameters
        ----------
        date: str (default "EMITTED_AT"):
            Date column to filter on.
        date_from: date (default None):
            Date from to get data, format "%Y-%m-%d".
        date_to: date (default None):
            Date to to get data, format "%Y-%m-%d".
        """
        # Get organizations
        df_organisations = Organizations.get(self)

        # For each bank account, get all transactions
        df_transaction = pd.DataFrame()
        for _, row in df_organisations.iterrows():
            iban = row["IBAN"]

            # Get transactions
            has_more = True
            current_page = 1
            while has_more:
                params = {
                    "current_page": current_page,
                    "iban": iban,
                }
                res = requests.get(
                    url=f"{QONTO_API_URL}/transactions?{urllib.parse.urlencode(params, safe='(),')}",
                    headers=self.headers,
                )
                try:
                    res.raise_for_status()
                except requests.HTTPError as e:
                    return e
                items = res.json()
                transactions = items.get("transactions")
                df = pd.DataFrame.from_records(transactions)
                df["iban"] = iban
                df_transaction = pd.concat([df_transaction, df], axis=0)
                # Check if next page exists
                next_page = items.get("meta").get("next_page")
                if next_page is None:
                    has_more = False
                else:
                    current_page = int(next_page)

        # Formatting
        df_transaction["transaction_order"] = df_transaction.apply(
            lambda row: int(row["transaction_id"].split("-")[-1]), axis=1
        )
        df_transaction = df_transaction.sort_values(
            by=["iban", "transaction_order"]
        ).reset_index(drop=True)

        # Select columns to keep
        to_keep = [
            "iban",
            "settled_at",
            "emitted_at",
            "transaction_id",
            "transaction_order",
            "label",
            "status",
            "category",
            "reference",
            "operation_type",
            "card_last_digits",
            "side",
            "amount",
            "currency",
            "vat_amount",
            "vat_rate",
            "local_amount",
            "local_currency",
            "note",
            "attachment_ids",
            "attachment_lost",
            "attachment_required",
        ]
        df_transaction = df_transaction[to_keep]

        # Sign amounts
        df_transaction.loc[
            df_transaction["side"] == "debit", "amount"
        ] = df_transaction["amount"] * (-1)
        df_transaction.loc[
            df_transaction["side"] == "debit", "local_amount"
        ] = df_transaction["local_amount"] * (-1)
        df_transaction.loc[
            df_transaction["side"] == "debit", "vat_amount"
        ] = df_transaction["vat_amount"] * (-1)

        # Format columns in upper case
        df_transaction = df_transaction.fillna("Not defined")
        df_transaction.columns = df_transaction.columns.str.upper()

        # Filter dataframe
        df_transaction = Qonto.filter_dates(
            df_transaction, date, date_from=date_from, date_to=date_to
        )
        return df_transaction


class Statements(Transactions):
    def __init__(self, user_id, headers):
        Qonto.__init__(self)
        self.user_id = user_id
        self.headers = headers

    def get(
        self,
        date="EMITTED_AT",
        to_group=[
            "IBAN",
            "DATE",
            "TRANSACTION_ID",
            "TRANSACTION_ORDER",
            "LABEL",
            "REFERENCE",
            "CATEGORY",
            "OPERATION_TYPE",
            "CURRENCY",
        ],
        date_from=None,
        date_to=None,
    ):
        """
        Return an dataframe object with 11 columns:
        - IBAN
        - DATE
        - TRANSACTION_ID
        - TRANSACTION_ORDER
        - LABEL
        - REFERENCE
        - CATEGORY
        - OPERATION_TYPE
        - CURRENCY
        - AMOUNT
        - POSITION

        Parameters
        ----------
        date: str (default "EMITTED_AT"):
            Date column to filter on.
        date_from: date (default None):
            Date from to get data, format "%Y-%m-%d".
        date_to: date (default None):
            Date to to get data, format "%Y-%m-%d".
        """

        # Get transactions
        df = Transactions.get(self, date)

        # Set date column
        df = df.rename(columns={date: "DATE"})
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.strftime(DATE_FORMAT)

        # Groupby
        if "DATE" not in to_group:
            to_group = ["DATE"] + to_group
        if "IBAN" not in to_group:
            to_group = ["IBAN"] + to_group
        df = df.groupby(to_group, as_index=False).agg({"AMOUNT": "sum"})

        # Calc position
        if "TRANSACTION_ORDER" in to_group:
            df = df.sort_values(by=["IBAN", "TRANSACTION_ORDER"]).reset_index(drop=True)
        else:
            df = df.sort_values(by=["IBAN", "DATE"]).reset_index(drop=True)
        df_statement = pd.DataFrame()
        ibans = df["IBAN"].unique()
        for iban in ibans:
            tmp = df[df["IBAN"] == iban]
            tmp["POSITION"] = tmp.agg({"AMOUNT": "cumsum"})
            df_statement = pd.concat([df_statement, tmp])

        # Filter dataframe
        df_statement = Qonto.filter_dates(
            df_statement, date_column="DATE", date_from=date_from, date_to=date_to
        )
        return df_statement

    def barline(
        self,
        to_group=["IBAN", "DATE"],
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
        df = self.get(to_group=to_group, date_from=date_from, date_to=date_to)
        df_line = df.copy()
        df_line = df_line[["DATE", "POSITION"]]
        df_line["DATE"] = pd.to_datetime(df_line["DATE"])

        # Data bar
        df_bar = self.get(
            to_group=["TRANSACTION_ID", "LABEL", "OPERATION_TYPE"],
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

        # Title
        if title is None:
            # Get date from and date to if None
            if date_from is None:
                date_from = df.DATE.unique().min()
            if date_to is None:
                date_to = df.DATE.unique().max()
            # Format date
            date_from = datetime.strptime(date_from, DATE_FORMAT).strftime("%d/%m/%Y")
            date_to = datetime.strptime(date_to, DATE_FORMAT).strftime("%d/%m/%Y")
            # Create title
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
        fig.update_layout(
            plot_bgcolor="#ffffff",
            hovermode="x",
            title=title,
            title_font=dict(family="Arial", size=18, color="black"),
            width=1200,
            height=800,
            margin_pad=10,
        )
        fig.update_yaxes(tickprefix="€", gridcolor="#eaeaea")
        fig.update_xaxes(dtick="M1")
        return fig

    def summary(self, summary_type, language="EN", date_from=None, date_to=None):
        # Get data
        df_statement = self.get(
            to_group=[summary_type], date_from=date_from, date_to=date_to
        )

        # Create
        if date_from is None:
            date_from = df_statement.DATE.unique().min()
        if date_to is None:
            date_to = df_statement.DATE.unique().max()

        # > Get first position
        first_position = round(df_statement["POSITION"].tolist()[0], 2)
        first_text = f"{datetime.strptime(date_from, DATE_FORMAT).strftime('%d/%m/%Y')}"
        df_first = pd.DataFrame(
            [{summary_type: first_text, "ORDER": 0, "AMOUNT": first_position}]
        )
        # > Get last position
        last_position = round(df_statement["POSITION"].tolist()[-1], 2)
        current_text = f"{datetime.strptime(date_to, DATE_FORMAT).strftime('%d/%m/%Y')}"
        df_last = pd.DataFrame(
            [{summary_type: current_text, "ORDER": 99999, "AMOUNT": last_position}]
        )

        # > Get transactions
        df_transaction = df_statement.copy()

        # Order
        if summary_type == "OPERATION_TYPE":
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
            df_transaction["ORDER"] = df_transaction[summary_type].replace(
                transaction_order
            )
        else:
            values = sorted(df_transaction[summary_type].unique())
            transaction_order = {}
            for i, v in enumerate(values):
                transaction_order[v] = i
            df_transaction["ORDER"] = df_transaction[summary_type].replace(
                transaction_order
            )

        # Label
        if language == "FR":
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
            df_transaction[summary_type] = df_transaction[summary_type].replace(
                transaction_type
            )
            value_col = "Montant"
        else:
            df_transaction[summary_type] = " - " + df_transaction[
                summary_type
            ].str.capitalize().str.replace("_", " ")
            value_col = "Amount"

        # Groupby month
        to_group = [summary_type, "ORDER"]
        to_agg = {"AMOUNT": "sum"}
        df_transaction = df_transaction.groupby(to_group, as_index=False).agg(to_agg)

        # Create summary table
        cash_summary = pd.concat([df_first, df_transaction, df_last], axis=0)
        cash_summary = cash_summary.sort_values("ORDER").drop("ORDER", axis=1)
        cash_summary["AMOUNT"] = (
            cash_summary["AMOUNT"].map(NUMBER_FORMAT.format).str.replace(",", " ")
        )
        cash_summary = cash_summary.rename(
            columns={summary_type: "Type", "AMOUNT": value_col}
        )
        return cash_summary

    def transactions(self, date_from=None, date_to=None):
        # Data
        df = self.get(
            to_group=["LABEL", "OPERATION_TYPE"], date_from=date_from, date_to=date_to
        )

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
        email_content = emailbuilder.generate(display="iframe", **content)
        return email_content
