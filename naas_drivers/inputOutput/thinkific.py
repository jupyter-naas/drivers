from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
import os


class TKCRUD:
    def __init__(self, base_url, subdomain, auth):
        self.req_headers = {
            "X-Auth-API-Key": auth,
            "X-Auth-Subdomain": subdomain,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.base_url = base_url
        self.model_name = self.base_url.split("/")[-1]

    def __get_by_page(self, page):
        data = {"page": page}
        req = requests.get(
            url=f"{self.base_url}/",
            headers=self.req_headers,
            json=data,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req.json()

    def get_all(self):
        items = []
        current_page = 1
        more_page = True
        while more_page:
            data = self.__get_by_page(current_page)
            items.extend(data.get("items"))
            total_pages = data.get("meta").get("pagination").get("total_pages") or 0
            if current_page == total_pages:
                more_page = False
            current_page += 1
        df = pd.DataFrame.from_records(items)
        return df

    def get(self, uid):
        if not uid:
            raise ValueError("Uid can not be None.")

        try:
            req = requests.get(
                url=f"{self.base_url}/{uid}",
                headers=self.req_headers,
                allow_redirects=False,
            )
            req.raise_for_status()
            return req.json()
        except requests.HTTPError as err:
            err_code = err.response.status_code
            if err_code == 404:
                print(f"Uid '{uid}' not found.")
            else:
                print(err.response.json())

    def update(self, data):
        uid = data["id"]
        if not uid:
            raise ValueError("Uid can not be None.")
        try:
            req = requests.put(
                url=f"{self.base_url}/{uid}",
                headers=self.req_headers,
                json=data,
                allow_redirects=False,
            )
            req.raise_for_status()
            print(f"'{self.model_name}' '{uid}' updated.")
        except requests.HTTPError as err:
            err_code = err.response.status_code
            if err_code == 404:
                print(f"Uid '{uid}' not found.")
            else:
                print(err.response.json())

    def send(self, data):
        if len(data) == 0:
            raise ValueError("Data is empty")

        try:
            req = requests.post(
                url=f"{self.base_url}/",
                headers=self.req_headers,
                json=data,
                allow_redirects=False,
            )
            req.raise_for_status()
            return req.json()
        except requests.HTTPError as err:
            err_code = err.response.status_code
            if err_code == 422:
                print("User already exists.")
            else:
                print(err.response.json())

    def delete(self, uid):
        try:
            req = requests.delete(
                url=f"{self.base_url}/{uid}",
                headers=self.req_headers,
                allow_redirects=False,
            )
            req.raise_for_status()
            print(f"'{self.model_name}' '{uid}' delete.")
        except requests.HTTPError as err:
            err_code = err.response.status_code
            if err_code == 404:
                print(f"Uid '{uid}' not found.")
            else:
                print(err.response.json())


class User(TKCRUD):
    def create(self, email, password, first_name, last_name, company=None):
        user = {
            "email": email,
            "password": password,
            "first_name": first_name,
            "last_name": last_name,
            "company": company,
        }
        result = self.send(user)
        return result


class Courses(TKCRUD):
    def get_chapters(self, uid):
        req = requests.get(
            url=f"{self.base_url}/{uid}/chapters",
            headers=self.req_headers,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req.json()


class Thinkific(InDriver, OutDriver):

    base_url = os.environ.get(
        "THINKIFIC_API_URL", "https://api.thinkific.com/api/public/v1"
    )
    token = None
    subdomain = None
    users = None
    enrollments = None

    def connect(self, api_token, subdomain):
        # Init thinkific attribute
        self.token = api_token
        self.subdomain = subdomain

        # Init end point
        self.users = User(f"{self.base_url}/users", self.subdomain, self.token)
        self.enrollments = TKCRUD(
            f"{self.base_url}/enrollments", self.subdomain, self.token
        )
        self.courses = Courses(f"{self.base_url}/courses", self.subdomain, self.token)
        self.reviews = TKCRUD(
            f"{self.base_url}/course_reviews", self.subdomain, self.token
        )

        # Set connexion to active
        self.connected = True
        return self
