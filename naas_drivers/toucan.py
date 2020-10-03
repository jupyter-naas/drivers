import requests
import shutil
import os
import io
import json
import time
import pandas as pd
import cson
import datetime
from typing import Any, Dict, List
import jwt
import uuid
from IPython.core.display import display, HTML


class Toucan:
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

    def __init__(self, url, login, mode="prod"):
        self.url_base = url
        host = url.partition("://")[2]
        self.__url_name = host.partition(".")[0]
        self.login = login
        self.mode = mode
        try:
            if self.mode == "debug":
                print("Get toucan tc-params")
            self.tc_params = self.__request_tc_params()
            if self.mode == "debug":
                print("Get toucan url_api")
            self.url_api = self.tc_params.get("API_BASEROUTE")
            if self.mode == "debug":
                print("Get toucan user")
            self.user = self.__request_user()
            if self.mode == "debug":
                print("Get toucan token")
            self.__token = self.user.get("token")
            if self.mode == "debug":
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
        if self.mode == "debug":
            print("user logged")

    def get_headers(self):
        return {"authorization": f"Bearer {self.__token}"}

    def __request_small_apps(self):
        req = requests.get(
            f"{self.url_api}/{self.__url_small_apps}", headers=self.get_headers()
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
        jsonConf = json.loads(res)
        return jsonConf

    def craft_toucan_embed_token(
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

    def dashboardSelector(self):
        statusApp = self.get_version()
        version = statusApp["frontVersion"]
        if version == "v64.0.0":
            return ".dashboard-report"
        else:
            return ".small-app-home__content"

    def __generateUrls(self, app_name, config, reports, ids):
        arr = []
        if self.mode == "debug":
            print("Get app slides")
        slides = config.get("slides")
        if self.mode == "debug":
            print("Get app Home")
        home = config.get("home")
        if home is not None and "skipToReport" in home:
            arr.append(
                {
                    "url": f"{self.url_base}/{app_name}",
                    "selector": self.dashboardSelector(),
                    "name": "dashboard",
                }
            )
            if self.mode == "debug":
                print("Add New dashboard to url list")
        else:
            arr.append(
                {
                    "url": f"{self.url_base}/{app_name}",
                    "selector": ".report-execsum",
                    "name": "dashboard",
                }
            )
            if self.mode == "debug":
                print("Add Old dashboard to url list")
        if self.mode == "debug":
            print("Generate all url from config")
        for report in reports:
            if "id" in report and "entityName" in report:
                for slide in slides:
                    if "parent_id" in slide and "id" in slide and "title" in slide:
                        name = self.generateScreenShotName(slide, report)
                        rId = self.__calcReportId(report.get("id"), ids)
                        if self.mode == "debug":
                            print("Generate url for", name)
                        arr.append(
                            {
                                "url": f"{self.url_base}/{app_name}?report={rId}&dashboard={rId}&slide={slide.get('id')}",
                                "selector": ".tc-slide__content, .tc-story",
                                "name": name,
                            }
                        )
        return arr

    def get_version(self):
        reqApi = requests.get(f"{self.url_api}", headers=self.get_headers())
        reqApi.raise_for_status()
        result = reqApi.json()
        reqApp = requests.get(f"{self.url_base}/{self.__url_tc_app_version}")
        reqApp.raise_for_status()
        frontVersion = reqApp.text().strip()
        result["frontVersion"] = frontVersion
        return result

    def get_app_config(self, app_name):
        req = requests.get(
            f"{self.url_api}/{app_name}/{self.__url_config}", headers=self.get_headers()
        )
        req.raise_for_status()
        return req.json()

    def get_app_data(self, app_name):
        req = requests.get(
            f"{self.url_api}/{app_name}/{self.__url_data}", headers=self.get_headers()
        )
        req.raise_for_status()
        return req.json()

    def get_app_reportsIds(self, app_name):
        req = requests.get(
            f"{self.url_api}/{app_name}/{self.__url_reports}/{self.__ur_ids}",
            headers=self.get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def get_app_reports(self, app_name):
        req = requests.get(
            f"{self.url_api}/{app_name}/{self.__url_reports}",
            headers=self.get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def get_users(self):
        req = requests.get(
            f"{self.url_api}/{self.__url_users}", headers=self.get_headers()
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
        formatFile = "&format=cson"
        files = None
        data = None
        if format_file == "front_config":
            formatFile = "&format=cson"
            files = {"file": io.StringIO(json.dumps({"json": cson.load(file_upload)}))}
        elif format_file == "etl_config":
            config_name = "/etl"
            formatFile = "&format=cson"
            data = file_upload
        elif format_file == "report":
            config_name = "/report"
            formatFile = "&format=cson"
            data = file_upload
        elif format_file == "dashboard-Group":
            config_name = "/dashboard-Group"
            formatFile = "&format=cson"
            data = file_upload
        elif format_file == "augment.py":
            config_name = "/augment"
            files = {"file": file_upload}
            formatFile = ""
        elif format_file == "preprocess_validation":
            config_name = "/preprocess_validation"
            formatFile = "&format=cson"
            data = file_upload
        elif format_file == "permissions.py":
            config_name = "/permissions"
            files = {"file": file_upload}
            formatFile = ""
        elif format_file == "permissions_config":
            config_name = "/permissions_config"
            formatFile = "&format=cson"
            data = file_upload
        elif format_file == "notifications_handlers.py":
            config_name = "/notifications_handlers"
            files = {"file": file_upload}
            formatFile = ""

        req = requests.put(
            f"{self.url_api}/{app_name}/{self.__url_config}{config_name}?stage={stage}{formatFile}",
            headers=self.get_headers(),
            files=files,
            data=data,
        )
        req.raise_for_status()
        return req.json()

    def download_app_config(
        self, app_name, format_file="front_config", stage="staging"
    ):
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
            formatFile = ""
        elif format_file == "preprocess_validation":
            config_name = "/preprocess_validation"
        elif format_file == "permissions.py":
            config_name = "/permissions"
            formatFile = ""
        elif format_file == "permissions_config":
            config_name = "/permissions_config"
        elif format_file == "notifications_handlers.py":
            config_name = "/notifications_handlers"
            formatFile = ""

        req = requests.get(
            f"{self.url_api}/{app_name}/{self.__url_config}{config_name}?stage={stage}{formatFile}",
            headers=self.get_headers(),
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
            headers=self.get_headers(),
            json={"operations": operations},
        )
        req.raise_for_status()
        return req.json()

    def get_data(self, app_name, domain, stage="staging"):
        req = requests.get(
            f"{self.url_api}/{app_name}/domain/{domain}?stage={stage}",
            headers=self.get_headers(),
        )
        req.raise_for_status()
        s_data = str(req.content, "utf-8")
        return io.StringIO(s_data)

    def get_metadata(self, app_name, stage="staging"):
        req = requests.get(
            f"{self.url_api}/{app_name}/metadata?stage={stage}",
            headers=self.get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def create_small_app(self, app_name, id_app=None):
        body = {"name": app_name, "id": id_app if id_app else app_name}
        req = requests.post(
            f"{self.url_api}/small-apps", headers=self.get_headers(), json=body
        )
        req.raise_for_status()
        return req.json()

    def embed_small_app_slide(self, small_app, slide, hosts=None):
        allowedHosts = (
            hosts
            if hosts
            else [
                os.environ.get("PUBLIC_PROXY_API", ""),
                os.environ.get("JUPYTERHUB_URL", ""),
            ]
        )
        uid = str(uuid.uuid4())
        data = {
            "allowedHosts": allowedHosts,
            "expirationDate": None,
            "layout": {
                "type": "single",
                "content": {"path": f"slides[?id==`{slide}`]", "variables": {}},
            },
            "paths": [f"slides[?id==`{slide}`]"],
            "public": False,
            "smallApp": small_app,
            "uid": uid,
            "variables": {},
        }
        req = requests.post(
            f"{self.url_api}/{self.__url_embed}", headers=self.get_headers(), json=data
        )
        req.raise_for_status()
        url = f"{self.url_base}/embedLauncher.js?id={uid}&token={self.__token}"
        display(HTML(f'<script async src="{url}" type="text/javascript"></script>'))

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
        notificationStr = "true" if notification else "false"
        req = requests.post(
            f"{self.url_api}/{app_name}/{self.__url_config}/operations?notify={notificationStr}&stage={stage}",
            headers=self.get_headers(),
            json={"operations": operations},
        )
        req.raise_for_status()
        return req.json()

    def load_conf(self, app_name, stage="staging"):
        req = requests.post(
            f"{self.url_api}/{app_name}/{self.__url_load}?stage={stage}",
            headers=self.get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def release_conf(self, app_name, stage="staging"):
        req = requests.post(
            f"{self.url_api}/{app_name}/{self.__url_release}?stage={stage}",
            headers=self.get_headers(),
        )
        req.raise_for_status()
        return req.json()

    def screenshotsAppAll(self):
        for app in self.small_apps:
            self.screenshotsApp(app)

    def getReportByName(self, reports, reportName):
        reportFound = None
        if not reports or len(reports) == 0:
            return reportFound
        for report in reports:
            if (
                reportName is not None
                and "id" in report
                and "entityName" in report
                and report.get("entityName") == reportName
            ):
                reportFound = report
        return reportFound

    def generateScreenShotName(self, slide, report):
        title = slide.get("title", "noTitle")
        title = "".join(x for x in title if x.isalnum())
        name = report.get("entityName", "noEntity")
        name = "".join(x for x in name if x.isalnum())
        return f"{name}_{title}"

    def __calcReportId(self, currentId, ids):
        calcId = currentId
        if ids is not None and 0 in ids:
            calcId += int(ids.get(0))
        return calcId

    def generateFolderName(self, app_name):
        today = datetime.date.today().strftime("%Y_%m_%d")
        return f"{today}/{self.__url_name}/{app_name}"

    def isAppAllowed(self, app_name):
        res = False
        for small_app in self.small_apps:
            if small_app.get("id") == app_name:
                res = True
        return res

    def __filterReport(self, reports, reportName):
        resReport = [{"id": 0, "entityName": "default"}]
        if reportName is None:
            return reports
        if not reports or len(reports) == 0:
            return resReport
        found = self.getReportByName(reports, reportName)
        if found:
            resReport = [found]
        return resReport

    def screenshotsApp(self, app_name, reportName=None, fullSize=False):
        if self.mode == "debug":
            print("Generate screenshots for", app_name)
        if self.isAppAllowed(app_name):
            folderName = self.generateFolderName(app_name)
            self.__createFolder(folderName)
            if self.mode == "debug":
                print("Get app config")
            config = self.get_app_config(app_name)
            ids = None
            try:
                ids = self.get_app_reportsIds(app_name)
            except requests.exceptions.RequestException:
                print("Cannot get report ids")
            reportsAll = self.get_app_reports(app_name)
            reports = self.__filterReport(reportsAll, reportName)
            urls = self.__generateUrls(app_name, config, reports, ids)
            for url in urls:
                req = ""
                path = f"{folderName}/{url.get('name')}.png"
                try:
                    if self.mode == "debug":
                        print(f"Request Screenshot {path}")
                    req = requests.get(
                        self.__url_screenshot_api,
                        params={
                            "url": url.get("url"),
                            "format": "png",
                            "lang": "en",
                            "fullSize": fullSize,
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
                path = f"{folderName}/{url.get('name')}.png"
                if req.status_code == 200:
                    with open(path, "wb") as out_file:
                        shutil.copyfileobj(req.raw, out_file)
                        if self.mode == "debug":
                            print(f"Created Screenshot {path}")
        else:
            print("this app is not allow to your account")
