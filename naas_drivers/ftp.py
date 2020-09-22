import ftplib
import pysftp
import io


class Ftp:
    """FTP/SFTP Connector class for cashstory\nSample Credentials file :{
        "url": "",
        "protocole": "ftp or sftp",
        "port": ,
        "type": "explicit TLS",
        "auth_method": "Passif or Actif",
        "username": "",
        "password": ""
    }"""

    def __enter__(self):
        return self

    def get_file(self, path):
        """Read and return data from file which is specified 'path' """
        if self.conprotocol == "ftp":
            temp_data = io.BytesIO()
            self.ftp.retrbinary("RETR " + path, temp_data.write)
            temp_data.seek(0)
            return temp_data
        elif self.conprotocol == "sftp":
            with self.ftp.open(path) as f:
                return f

    def list_directory(self, path):
        files = []
        try:
            files = self.ftp.nlst()
        except ftplib.error_perm:
            print("Error read this directory")
        return files

    def __init__(self, credentials):
        if credentials["protocole"] == "ftp":
            ftp = (
                ftplib.FTP_TLS(credentials["url"])
                if credentials["type"] == "explicit TLS"
                else ftplib.FTP(credentials["url"])
            )
            ftp.connect(credentials["url"], credentials["port"])
            ftp.set_pasv(True) if credentials[
                "auth_method"
            ] == "Passif" else ftp.set_pasv(False)
            ftp.login(credentials["username"], credentials["password"])
            self.ftp = ftp
        elif credentials["protocole"] == "sftp":
            self.ftp = pysftp.Connection(
                host=credentials["url"],
                username=credentials["username"],
                password=credentials["password"],
                port=credentials["port"],
            )
        self.conprotocol = credentials["protocole"]

    def __exit__(self, *krgs):
        self.ftp.close()

    def help(self):
        print(f"=== {type(self).__name__} === \n")
        print(".get_file(path) => get file from ftp path\n")
        print(".list_directory(path) => do ls in ftp in path\n")
