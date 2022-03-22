import ftplib
import os
import pysftp


class Ftpbase:
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

    def get_file(self, path, dest_path=None):
        """Read and return data from file which is specified 'path'"""
        saved_path = None
        filename = os.path.basename(path)
        if filename != path:
            saved_path = self.pwd()
            self.cwd(os.path.dirname(path))
        if self.conprotocol == "ftp":
            with open(dest_path if dest_path else filename, "wb") as f:

                def callback(data):
                    f.write(data)
                    print(f"Saved as {dest_path}")

                self.retrbinary(f"RETR {filename}", callback)
        elif self.conprotocol == "sftp":
            with self.ftp.open(path) as f:
                file_byte = open(dest_path if dest_path else path, "rb")  # file to send
                file_byte.write(f)
                file_byte.close()
                print(f"Saved as {dest_path}")

        if filename != path:
            self.cwd(saved_path)

    def send_file(self, path, dest_path=None):
        saved_path = None
        if dest_path is not None:
            saved_path = self.pwd()
            self.cwd(os.path.dirname(dest_path))
        filename = os.path.basename(path)
        ftp_command = f"STOR {filename}"
        file_byte = open(path, "rb")  # file to send
        self.storbinary(ftp_command, file_byte)  # send the file
        file_byte.close()
        if dest_path is not None:
            self.cwd(saved_path)
        print(f"Saved in {dest_path}")

    def list_directory(self, path):
        files = []
        try:
            files = self.ftp.nlst()
        except ftplib.error_perm:
            print("Error read this directory")
        return files

    def __init__(self, **credentials):
        if credentials["protocole"] == "ftp":
            ftp = (
                ftplib.FTP_TLS(credentials["url"])
                if credentials["type"] == "explicit TLS"
                else ftplib.FTP(credentials["url"])  # Sensitive
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
