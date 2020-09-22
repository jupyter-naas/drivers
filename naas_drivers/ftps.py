import ssl
import ftplib
import io
import datetime


class Ftps(ftplib.FTP_TLS):
    """FTPS subclass that automatically wraps sockets in SSL to support implicit FTPS."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        """Return the socket."""
        return self._sock

    @sock.setter
    def sock(self, value):
        """When modifying the socket, ensure that it is ssl wrapped."""
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value

    def get_file(self, path):
        """Read and return data from file which is specified 'path' """
        temp_data = io.BytesIO()
        self.retrbinary("RETR " + path, temp_data.write)
        temp_data.seek(0)
        return temp_data

    def list_directory(self, dirName):
        dirs = []
        self.dir(dirName, dirs.append)
        files = []
        for dirr in dirs:
            line = dirr.lstrip().split()
            datetmp = " ".join(line[5 : len(line) - 1])  # noqa: E203
            month, day, year = datetmp.split(" ")
            time = None
            now = datetime.datetime.now()
            if year.find(":") > -1:
                time = year
                year = now.year
            else:
                time = "00:00"
            files.append(
                {
                    "permission": line[0],
                    "links": line[1],
                    "owner": line[2],
                    "group": line[3],
                    "size": line[4],
                    "month": month,
                    "day": day,
                    "year": year,
                    "time": time,
                    "name": line[-1],
                }
            )
        return files

    def help(self):
        print(f"=== {type(self).__name__} === \n")
        print(".get_file(path) => get file from ftp path\n")
        print(".list_directory(path) => do ls in ftp in path\n")
