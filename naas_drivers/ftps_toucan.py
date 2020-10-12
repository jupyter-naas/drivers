from .ftps import Ftps


class Ftps_toucan:
    """FTPS subclass for toucan"""

    _ftps = None

    def __init__(self, user, passwd, host="ftps.toucantoco.com", port=990):
        self._ftps = Ftps()
        self._ftps.connect(host=host, port=port)
        self._ftps.login(user=user, passwd=passwd)

    def __before_all(self):
        self._ftps.prot_c()
        self._ftps.prot_p()

    def get_file(self, path):
        self.__before_all()
        return self._ftps.get_file(path)

    def send_file(self, path, dest_path=None):
        self.__before_all()
        self._ftps.send_file(path, dest_path)

    def list_directory(self, dirName):
        self.__before_all()
        return self._ftps.list_directory(dirName)

    def help(self):
        print(f"=== {type(self).__name__} === \n")
        print(".get_file(path) => get file from ftp path\n")
        print(".send_file(path, dest_path) => send file to ftp path\n")
        print(".list_directory(path) => do ls in ftp in path\n")
