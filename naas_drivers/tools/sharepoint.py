from os import path
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version


class Sharepoint:
    def connect(self, endpoint: str, username: str, password: str, site: str):
        self.endpoint = endpoint

        self.authcookie = Office365(
            f"https://{endpoint}", username=username, password=password
        ).GetCookies()
        self.site = Site(
            f"https://{endpoint}/sites/{site}",
            version=Version.v2016,
            authcookie=self.authcookie,
        )

        return self

    def list_folder(self, folder: str):
        f = self.site.Folder(folder)
        return {"folders": f.folders, "files": f.files}

    def get_file(self, src: str, dst: str):
        try:
            folder = self.site.Folder(path.dirname(src))
            with open(dst, "wb") as f:
                f.write(folder.get_file(path.basename(src)))
                f.close()
            print(f'✅ "{src}" downloaded in "{dst}"')
        except Exception as e:
            print(f'[red]❌ Error while downloading "{src}" into "{dst}"')
            print(e)

    def upload_file(self, src: str, dst: str):
        try:
            folder = self.site.Folder(path.dirname(dst))
            with open(src, mode="rb") as file:
                fileContent = file.read()
                folder.upload_file(fileContent, path.basename(dst))
            print(f'✅ "{src}" uploaded in "{dst}"')
        except Exception as e:
            print(f'[red]❌ Error while uploading "{src}" into "{dst}"')
            print(e)
