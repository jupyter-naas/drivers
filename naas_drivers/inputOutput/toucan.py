from naas_drivers.driver import InDriver, OutDriver
from IPython.core.display import display, HTML
from typing import Any, Dict, List
import pandas as pd
import requests
import datetime
import shutil
import json
import time
import uuid
import cson
import jwt
import os
import io


class Toucan(InDriver, OutDriver):
    """Toucan lib"""

    # Private vars
    __url_screenshot_api = os.environ.get(
        "TC_API_SCREENSHOT", "http://toucan-screenshot:3000/screenshot"
    )
    __TOUCAN_EMBED_ENCRYPTION_KEY = os.environ.get("TOUCAN_EMBED_ENCRYPTION_KEY", None)
    __url_config = "config"
    __url_load = "load"
    __url_embed = "embed"
    __url_release = "release"
    __url_reports = "reports"
    __url_data = "data"
    __url_login = "login"
    __ur_ids = "ids"
    __url_users = "users"
    __url_small_apps = "small-apps"
    __url_tc_params = "scripts/tc-params.js"
    __url_tc_app_version = "tucana-version.txt"
    __replace_tc_params = "window.TC_PARAMS = "
    __token = None
    __url_name = None
    # Public vars
    url_base = None
    url_api = None
    login = {}
    user = {}
    config = {}
    tc_params = {}
    small_apps = []
    debug = False

    def connect(self, url, username, password, debug=False):
        self.url_base = url
        host = url.partition("://")[2]
        self.__url_name = host.partition(".")[0]
        self.login = {"username": username, "password": password}
        self.debug = debug
        try:
            if self.debug:
                print("Get toucan tc-params")
            self.tc_params = self.__request_tc_params()
            if self.debug:
                print("Get toucan url_api")
            self.url_api = self.tc_params.get("API_BASEROUTE")
            if self.debug:
                print("Get toucan user")
            self.user = self.__request_user()
            if self.debug:
                print("Get toucan token")
            self.__token = self.user.get("token")
            if self.debug:
                print("Get toucan small_apps")
            self.small_apps = self.__request_small_apps()
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh.response.text)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            print("OOps: Something Else", err)
        if self.debug:
            print("user logged")
        self.connected = True
        return self

    def __request_small_apps(self):
        req = requests.get(
            f"{self.url_api}/{self.__url_small_apps}", headers=self.__get_headers()
        )
        req.raise_for_status()
        return req.json()

    def __request_tc_params(self):
        req = requests.get(f"{self.url_base}/{self.__url_tc_params}")
        req.raise_for_status()
        res = (
            req.text.replace(self.__replace_tc_params, "")
            .replace("\t", " ")
            .replace("\n", "")
            .replace(";", "")
        )
        json_conf = json.loads(res)
        return json_conf

    def __request_user(self):
        req = requests.post(f"{self.url_api}/{self.__url_login}", json=self.login)
        req.raise_for_status()
        return req.json()

    def __createFolder(self, name):
        try:
            os.makedirs(name)
            print(f"Successfully created the directory {name}")
        except OSError:
            print(f"Directory {name} already exist")

    def __calc_report_id(self, current_id, ids):
        calc_id = current_id
        if ids is not None and 0 in ids:
            calc_id += int(ids.get(0))
        return calc_id

    def __filterReport(self, reports, report_name):
        res_report = [{"id": 0, "entityName": "default"}]
        if report_name is None:
            return reports
        if not reports or len(reports) == 0:
            return res_report
        found = self.get_report_by_name(reports, report_name)
        if found:
            res_report = [found]
        return res_report

    def __generateUrls(self, app_name, config, reports, ids):
        arr = []
        if self.debug:
            print("Get app slides")
        slides = config.get("slides")
        if self.debug:
            print("Get app Home")
        home = config.get("home")
        if home is not None and "skipToReport" in home:
            arr.append(
                {
                    "url": f"{self.url_base}/{app_name}",
                    "selector": self.get_dashboard_selector(),
                    "name": "dashboard",
                }
            )
            if self.debug:
                print("Add New dashboard to url list")
        else:
            arr.append(
                {
                    "url": f"{self.url_base}/{app_name}",
                    "selector": ".report-execsum",
                    "name": "dashboard",
                }
            )
            if self.debug:
                print("Add Old dashboard to url list")
        if self.debug:
            print("Generate all url from config")
        for report in reports:
            if "id" in report and "entityName" in report:
                for slide in slides:
                    if "parent_id" in slide and "id" in slide and "title" in slide:
                        name = self.generate_screenshot_name(slide, report)
                        r_id = self.__calc_report_id(report.get("id"), ids)
                        if self.debug:
                            print("Generate url for", name)
                        arr.append(
                            {
                                "url": f"{self.url_base}/{app_name}?report={r_id}&dashboard={r_id}&slide={slide.get('id')}",
                                "selector": ".tc-slide__content, .tc-story",
                                "name": name,
                            }
                        )
        return arr

    def __get_headers(self):
        return {"authorization": f"Bearer {self.__token}"}

    def embed(self, small_app, slide, hosts=None, mode="webcomponent", height="800px"):
        allowed_hosts = (
            hosts
            if hosts
            else [
                os.environ.get("PUBLIC_PROXY_API", ""),
                os.environ.get("JUPYTERHUB_URL", ""),
            ]
        )
        uid = str(uuid.uuid4())
        data = {
            "allowedHosts": allowed_hosts,
            "expirationDate": None,
            "layout": {
                "type": "single",
                "content": {"path": f"slides[?id==`{slide}`]", "variables": {}},
            },
            "paths": [f"slides[?id==`{slide}`]"],
            "public": False,
            "smallApp": small_app,
            "uid": uid,
        }
        req = requests.post(
            f"{self.url_api}/{self.__url_embed}",
            headers=self.__get_headers(),
            json=data,
        )
        req.raise_for_status()
        html = None
        if mode == "webcomponent":
            html = f"""
                <div style="height:{height}">
                <script async src="{self.url_base}/scripts/embedLauncher.js?id={uid}&token={self.__token}" type="text/javascript"></script>
                </div>
            """
        else:
            html = f"""
                <div style="height:{height}">
                <iframe style="border: 0; overflow: hidden;" frameBorder="0" height="100%" width="100%"
                    src="{self.url_base}/embed.html?id=id={uid}&token={self.__token}"></iframe>
                </div>
            """
        display(HTML(html))
        return html

    def craft_token(
        self,
        username: str,
        small_apps_access: Dict[str, str],
        groups: List[str] = None,
        extra_infos: Dict[str, Any] = None,
        expires_in: datetime.timedelta = datetime.timedelta(hours=1),
    ) -> str:
        user_payload = {
            "username": username,
            "roles": ["USER"],
            "privileges": {"smallApp": small_apps_access},
            "groups": groups,
            "attributes": extra_infos,
        }
        payload = {
            **user_payload,
            "iat": datetime.datetime.utcnow(),
            "exp": datetime.datetime.utcnow() + expires_in,
        }
        return jwt.encode(
            payload, self.__TOUCAN_EMBED_ENCRYPTION_KEY, algorithm="HS256"
        ).decode("utf8")

    def get_dashboard_selector(self):
        status_app = self.get_version()
        version = status_app["frontVersion"]
        if version == "v64.0.0":
            return ".dashboard-report"
        else:
            return ".small-app-home__content"

    def get_version(self):
        req_api = requests.get(f"{self.url_api}", headers=self.__get_headers())
        req_api.raise_for_status()
        result = req_api.json()
        req_app = requests.get(f"{self.url_base}/{self.__url_tc_app_version}")
        req_app.raise_for_status()
        front_version = req_app.text().strip()
        result["frontVersion"] = front_version
        return result

    def get_app_config(self, app_name):
        req = requests.get(
            f"{self.url_api}/{app_name}/{self.__url_config}",
            headers=self.__get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def get_app_data(self, app_name):
        req = requests.get(
            f"{self.url_api}/{app_name}/{self.__url_data}", headers=self.__get_headers()
        )
        req.raise_for_status()
        return req.json()

    def get_app_reports_ids(self, app_name):
        req = requests.get(
            f"{self.url_api}/{app_name}/{self.__url_reports}/{self.__ur_ids}",
            headers=self.__get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def get_app_reports(self, app_name):
        req = requests.get(
            f"{self.url_api}/{app_name}/{self.__url_reports}",
            headers=self.__get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def get_users(self):
        req = requests.get(
            f"{self.url_api}/{self.__url_users}", headers=self.__get_headers()
        )
        req.raise_for_status()
        return req.json()

    def get_users_analytics(self):
        users = self.get_users()
        flatten_users = []
        for user in users:
            if user.get("logins"):
                for login in user.get("logins"):
                    date = time.ctime(login.get("date").get("$date") / 1000)
                    TIME = datetime.datetime.strptime(
                        date, "%a %b %d %H:%M:%S %Y"
                    ).strftime("%H:%M:%S")
                    DATE = datetime.datetime.strptime(
                        date, "%a %b %d %H:%M:%S %Y"
                    ).strftime("%Y-%m-%d")
                    flatten = {"USER": user.get("username"), "TIME": TIME, "DATE": DATE}
                    flatten_users.append(flatten)
            else:
                flatten = {"USER": user.get("username"), "TIME": None, "DATE": None}
                flatten_users.append(flatten)

        df = (
            pd.DataFrame(flatten_users)
            .drop(["TIME"], axis=1)
            .drop_duplicates()
            .reset_index(drop=True)
        )
        df["VALUE"] = 1

        df_session = df.copy()
        df_session = df_session.groupby(["USER", "DATE"], as_index=False).count()

        df_session_all = df.copy()
        df_session_all = (
            df_session_all.drop(["USER"], axis=1)
            .groupby(["DATE"], as_index=False)
            .count()
        )
        df_session_all["USER"] = "All users"

        df_final = pd.concat([df_session, df_session_all], axis=0)
        return df_final

    def send_app_config(
        self, app_name, file_upload, format_file="front_config", stage="staging"
    ):
        config_name = ""
        format_url = "&format=cson"
        files = None
        data = None
        if format_file == "front_config":
            files = {"file": io.StringIO(json.dumps({"json": cson.load(file_upload)}))}
        elif format_file == "etl_config":
            config_name = "/etl"
            data = file_upload
        elif format_file == "report":
            config_name = "/report"
            data = file_upload
        elif format_file == "dashboard-Group":
            config_name = "/dashboard-Group"
            data = file_upload
        elif format_file == "augment.py":
            config_name = "/augment"
            files = {"file": file_upload}
            format_url = ""
        elif format_file == "preprocess_validation":
            config_name = "/preprocess_validation"
            data = file_upload
        elif format_file == "permissions.py":
            config_name = "/permissions"
            files = {"file": file_upload}
            format_url = ""
        elif format_file == "permissions_config":
            config_name = "/permissions_config"
            data = file_upload
        elif format_file == "notifications_handlers.py":
            config_name = "/notifications_handlers"
            files = {"file": file_upload}
            format_url = ""

        req = requests.put(
            f"{self.url_api}/{app_name}/{self.__url_config}{config_name}?stage={stage}{format_url}",
            headers=self.__get_headers(),
            files=files,
            data=data,
        )
        req.raise_for_status()
        return req.json()

    def download_app_config(
        self, app_name, format_file="front_config", stage="staging"
    ):
        format_url = ""
        config_name = ""
        format_file = "&format=cson"
        if format_file == "etl_config":
            config_name = "/etl"
        elif format_file == "report":
            config_name = "/report"
        elif format_file == "dashboard-Group":
            config_name = "/dashboard-Group"
        elif format_file == "augment.py":
            config_name = "/augment"
            format_url = ""
        elif format_file == "preprocess_validation":
            config_name = "/preprocess_validation"
        elif format_file == "permissions.py":
            config_name = "/permissions"
            format_url = ""
        elif format_file == "permissions_config":
            config_name = "/permissions_config"
        elif format_file == "notifications_handlers.py":
            config_name = "/notifications_handlers"
            format_url = ""

        req = requests.get(
            f"{self.url_api}/{app_name}/{self.__url_config}{config_name}?stage={stage}{format_url}",
            headers=self.__get_headers(),
        )
        req.raise_for_status()
        return req

    def deploy_app(
        self,
        app_name,
        stage="staging",
        operations=[
            "preprocess_data_sources",
            "populate_reports",
            "populate_dashboards",
            "release_design",
        ],
        force=True,
    ):
        force_str = "true" if force else "false"
        req = requests.post(
            f"{self.url_api}/{app_name}/{self.__url_config}/pull?force={force_str}&stage={stage}",
            headers=self.__get_headers(),
            json={"operations": operations},
        )
        req.raise_for_status()
        return req.json()

    def get_data(self, app_name, domain, stage="staging"):
        req = requests.get(
            f"{self.url_api}/{app_name}/domain/{domain}?stage={stage}",
            headers=self.__get_headers(),
        )
        req.raise_for_status()
        s_data = str(req.content, "utf-8")
        return io.StringIO(s_data)

    def get_metadata(self, app_name, stage="staging"):
        req = requests.get(
            f"{self.url_api}/{app_name}/metadata?stage={stage}",
            headers=self.__get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def create_small_app(self, app_name, id_app=None):
        body = {"name": app_name, "id": id_app if id_app else app_name}
        req = requests.post(
            f"{self.url_api}/small-apps", headers=self.__get_headers(), json=body
        )
        req.raise_for_status()
        return req.json()

    def load_operations(
        self,
        app_name,
        stage="staging",
        notification=False,
        operations=[
            "preprocess_data_sources",
            "populate_basemaps",
            "populate_reports",
            "populate_dashboards",
            "populate_permissions",
        ],
    ):
        notification_str = "true" if notification else "false"
        req = requests.post(
            f"{self.url_api}/{app_name}/{self.__url_config}/operations?notify={notification_str}&stage={stage}",
            headers=self.__get_headers(),
            json={"operations": operations},
        )
        req.raise_for_status()
        return req.json()

    def load_conf(self, app_name, stage="staging"):
        req = requests.post(
            f"{self.url_api}/{app_name}/{self.__url_load}?stage={stage}",
            headers=self.__get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def release_conf(self, app_name, stage="staging"):
        req = requests.post(
            f"{self.url_api}/{app_name}/{self.__url_release}?stage={stage}",
            headers=self.__get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def get_report_by_name(self, reports, report_name):
        report_found = None
        if not reports or len(reports) == 0:
            return report_found
        for report in reports:
            if (
                report_name is not None
                and "id" in report
                and "entityName" in report
                and report.get("entityName") == report_name
            ):
                report_found = report
        return report_found

    def screenshots_app_all(self):
        for app in self.small_apps:
            self.screenshots_app(app)

    def generate_screenshot_name(self, slide, report):
        title = slide.get("title", "noTitle")
        title = "".join(x for x in title if x.isalnum())
        name = report.get("entityName", "noEntity")
        name = "".join(x for x in name if x.isalnum())
        return f"{name}_{title}"

    def generate_folder_name(self, app_name):
        today = datetime.date.today().strftime("%Y_%m_%d")
        return f"{today}/{self.__url_name}/{app_name}"

    def is_app_allowed(self, app_name):
        res = False
        for small_app in self.small_apps:
            if small_app.get("id") == app_name:
                res = True
        return res

    def screenshots_app(self, app_name, report_name=None, full_size=False):
        if self.debug:
            print("Generate screenshots for", app_name)
        if self.is_app_allowed(app_name):
            folder_name = self.__generate_folder_name(app_name)
            self.__createFolder(folder_name)
            if self.debug:
                print("Get app config")
            config = self.get_app_config(app_name)
            ids = None
            try:
                ids = self.get_app_reports_ids(app_name)
            except requests.exceptions.RequestException:
                print("Cannot get report ids")
            reports_all = self.get_app_reports(app_name)
            reports = self.__filterReport(reports_all, report_name)
            urls = self.__generateUrls(app_name, config, reports, ids)
            for url in urls:
                req = ""
                path = f"{folder_name}/{url.get('name')}.png"
                try:
                    if self.debug:
                        print(f"Request Screenshot {path}")
                    req = requests.get(
                        self.__url_screenshot_api,
                        params={
                            "url": url.get("url"),
                            "format": "png",
                            "lang": "en",
                            "fullSize": full_size,
                            "deviceScaleFactor": 1,
                            "token": self.__token,
                            "elementSelector": url.get("selector"),
                        },
                        stream=True,
                    )
                    req.raise_for_status()
                except requests.exceptions.HTTPError as errh:
                    print("Http Error:", errh.response.text)
                except requests.exceptions.ConnectionError as errc:
                    print("Error Connecting:", errc)
                except requests.exceptions.Timeout as errt:
                    print("Timeout Error:", errt)
                except requests.exceptions.RequestException as err:
                    print("OOps: Something Else", err)
                path = f"{folder_name}/{url.get('name')}.png"
                if req.status_code == 200:
                    with open(path, "wb") as out_file:
                        shutil.copyfileobj(req.raw, out_file)
                        if self.debug:
                            print(f"Created Screenshot {path}")
        else:
            print("this app is not allow to your account")
