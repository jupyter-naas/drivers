from .ftpbase import Ftpbase
from .ftpsbase import Ftpsbase


class Ftp:
    """FTP subclass"""

    _ftp = None

    def connect(
        self,
        user,
        passwd,
        host="ftps.toucantoco.com",
        port=990,
        secure=False,
        force_prot=False,
    ):
        if secure:
            self._ftp = Ftpsbase()
        else:
            self._ftp = Ftpbase()
        self._ftp.connect(host=host, port=port)
        self._ftp.login(user=user, passwd=passwd)
        self.__force_prot = force_prot

    def __before_all(self):
        if self.__force_prot:
            self._ftp.prot_c()
            self._ftp.prot_p()

    def get_file(self, path):
        self.__before_all()
        return self._ftp.get_file(path)

    def send_file(self, path, dest_path=None):
        self.__before_all()
        self._ftp.send_file(path, dest_path)

    def list_directory(self, dirName):
        self.__before_all()
        return self._ftp.list_directory(dirName)

    def help(self):
        print(f"=== {type(self).__name__} === \n")
        print(".get_file(path) => get file from ftp path\n")
        print(".send_file(path, dest_path) => send file to ftp path\n")
        print(".list_directory(path) => do ls in ftp in path\n")
