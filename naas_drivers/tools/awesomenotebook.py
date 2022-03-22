from naas_drivers.driver import InDriver
import requests
import urllib.parse
import traceback
from IPython.core.display import display, Markdown


class AwesomeNotebooks(InDriver):

    __repo = "jupyter-naas/awesome-notebooks"
    __naas_dl = "https://app.naas.ai/user-redirect/naas/downloader?url="
    __api_url = "https://api.github.com/repos/{REPO}/git/trees/{BRANCH}?recursive=1"
    __base_url = "https://github.com/{REPO}/blob/{BRANCH}/"
    __badge_base = "https://img.shields.io/badge/"
    __badge_appearance = "-Open%20in%20Naas-success?labelColor=000000"
    __badge_link = "https://naas.ai&link=https://app.naas.ai/user-redirect/naas/downloader?url={DLURL}"
    __badge_logo = """&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyB
3aWR0aD0iMTAyNHB4IiBoZWlnaHQ9IjEwMjRweCIgdmlld0JveD0iMCAwIDEwMjQgMTAyNCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwM
DAvc3ZnIiB4bWxuczp4bGluaz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluayIgdmVyc2lvbj0iMS4xIj4KIDwhLS0gR2VuZXJhdGVkIGJ
5IFBpeGVsbWF0b3IgUHJvIDIuMC41IC0tPgogPGRlZnM+CiAgPHRleHQgaWQ9InN0cmluZyIgdHJhbnNmb3JtPSJtYXRyaXgoMS4wIDAuMCAwL
jAgMS4wIDIyOC4wIDU0LjUpIiBmb250LWZhbWlseT0iQ29tZm9ydGFhLVJlZ3VsYXIsIENvbWZvcnRhYSIgZm9udC1zaXplPSI4MDAiIHRleHQt
ZGVjb3JhdGlvbj0ibm9uZSIgZmlsbD0iI2ZmZmZmZiIgeD0iMS4xOTk5OTk5OTk5OTk5ODg2IiB5PSI3MDUuMCI+bjwvdGV4dD4KIDwvZGVmcz4
KIDx1c2UgaWQ9Im4iIHhsaW5rOmhyZWY9IiNzdHJpbmciLz4KPC9zdmc+Cg=="""
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

    def __get_file_list(self, branch):
        url = self.__api_url.replace("{REPO}", self.__repo).replace("{BRANCH}", branch)
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
                        "{BRANCH}", branch
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

    def badge(self, url):
        badge_url = self.__badge_base + self.__badge_appearance + self.__badge_logo
        redirect_url = self.__badge_link.replace("{DLURL}", url)
        html_content = f"""<a href="{redirect_url}" target="_parent">\n<img src="{badge_url}"/>\n</a>"""
        display(Markdown(html_content))
        return html_content

    def get(self, md=True, open_in_naas=True, branch="master"):
        self.check_connect()
        if not md:
            return self.__get_file_list(branch)
        else:
            arr = self.__get_file_list(branch)
            text_md = self.__create_md(arr, open_in_naas)
            display(Markdown(text_md))
            return
