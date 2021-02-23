from naas_drivers.driver import InDriver
import requests
import urllib.parse
import traceback


class AwesomeNotebooks(InDriver):

    __branch = "master"
    __repo = "jupyter-naas/awesome-notebooks"
    __naas_dl = "https://app.naas.ai/user-redirect/naas/downloader?url="
    __api_url = "https://api.github.com/repos/{REPO}/git/trees/{BRANCH}?recursive=1"
    __base_url = "https://github.com/{REPO}/blob/{BRANCH}/"
    __TOC_LIST_PREFIX = "-"

    def __create_md(self, list_files, naas_open):
        """create markdown index of all notebook files in cwd and sub folders"""
        md_lines = []
        last_folder = None
        for file in list_files:
            folders = file.get("folders")
            level = folders.count("/") + 1
            indent = "  " * level
            if not last_folder or folders != last_folder:
                indent = "  " * (level - 1)
                folder = folders.split("/")[-1]
                last_folder = folders
                md_lines.append(f"{indent} {self.__TOC_LIST_PREFIX} **{folder}**\n")
            url_path = file.get("url")
            md_filename = file.get("filename")
            indent = "  " * level
            open_url = url_path
            if naas_open:
                open_url = self.__naas_dl + url_path
            md_lines.append(
                f"{indent} {self.__TOC_LIST_PREFIX} [{md_filename}]({open_url})\n"
            )

        return "".join(md_lines)

    def __get_file_list(self):
        url = self.__api_url.replace("{REPO}", self.__repo).replace(
            "{BRANCH}", self.__branch
        )
        files_list = []
        try:
            r = requests.get(url)
            data = r.json()
            for ff in data.get("tree"):
                path = ff.get("path")
                if (
                    not path.startswith(".")
                    and path.endswith(".ipynb")
                    and not path.endswith("generatereadme.ipynb")
                ):
                    base = self.__base_url.replace("{REPO}", self.__repo).replace(
                        "{BRANCH}", self.__branch
                    )
                    good_url = f"{base}{urllib.parse.quote(path)}"
                    folders = path.split("/")
                    filename = folders.pop()
                    if len(folders) > 0:
                        main_name = folders[0]
                        folders = "/".join(folders)
                        folders = folders.replace("_", " ")
                        filename = filename.replace(f"{main_name}", "")
                        filename = filename.replace("_", " ")
                        filename = filename.replace(".ipynb", "")
                        filename = filename.strip()
                        files_list.append(
                            {"folders": folders, "filename": filename, "url": good_url}
                        )
        except Exception as e:
            print("__get_onboarding_list", e)
            traceback.print_exc()
        return files_list

    def connect(self, repo):
        self.__repo = repo if repo else self.__repo
        self.connected = True
        return self

    def get(self, md=True, open_in_naas=True):
        self.check_connect()
        if not md:
            return self.__get_file_list()
        else:
            arr = self.__get_file_list()
            text_md = self.__create_md(arr, open_in_naas)
            return text_md
