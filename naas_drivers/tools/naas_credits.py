from naas_drivers.driver import InDriver, OutDriver
from naas_drivers.tools.naas_auth import NaasAuth
from datetime import datetime
import requests as r

CREDITS_API_FQDN = "credits.naas.ai"
AUTH_API_PROTOCOL = "https"


class NaasCredits(InDriver, OutDriver):
    __access_token = None
    __headers = None

    def __init__(self):
        self.transactions = self.Transactions(self)

    @property
    def headers(self):
        return self.__headers

    def connect(self, token=None):
        naas_auth = NaasAuth().connect(token)
        self.__access_token = naas_auth.access_token
        self.__headers = naas_auth.headers
        return self

    def get_plan(self):
        res = r.get(
            f"{AUTH_API_PROTOCOL}://{CREDITS_API_FQDN}/plan", headers=self.headers
        )
        res.raise_for_status()
        return res.json()

    def get_balance(self):
        res = r.get(
            f"{AUTH_API_PROTOCOL}://{CREDITS_API_FQDN}/balance", headers=self.headers
        )
        res.raise_for_status()
        return res.json()

    class __InnerBase:
        """__InnerBase class"""

        def __init__(self, parent):
            self.__parent = parent

        @property
        def headers(self):
            return self.__parent.headers

    class Transactions(__InnerBase):
        def get_currents(self):
            res = r.get(
                f"{AUTH_API_PROTOCOL}://{CREDITS_API_FQDN}/transactions/current",
                headers=self.headers,
            )
            res.raise_for_status()
            return res.json()

        def get(
            self,
            page_size: int = None,
            page_number: int = None,
            start_date: datetime = None,
            end_date: datetime = None,
            order_by: str = None,
        ):
            params = {
                "page_size": page_size,
                "page_number": page_number,
                "start_date": start_date,
                "end_date": end_date,
                "order_by": order_by,
            }
            params = {k: params[k] for k in params.keys() if params[k] is not None}

            res = r.get(
                f"{AUTH_API_PROTOCOL}://{CREDITS_API_FQDN}/transactions",
                headers=self.headers,
                params=params,
            )
            res.raise_for_status()
            return res.json()

        def import_bulk(self, username, files):
            res = r.post(
                f"{AUTH_API_PROTOCOL}://{CREDITS_API_FQDN}/transactions/import/bulk",
                headers=self.headers,
                files=files,
                params={"username": username},
            )
            res.raise_for_status()
            return res.json()
