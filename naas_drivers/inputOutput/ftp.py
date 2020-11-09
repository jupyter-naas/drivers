from naas_drivers.driver import InDriver, OutDriver
from .__ftp import Ftpbase
from .__ftps import Ftpsbase


class Ftp(InDriver, OutDriver):
    """FTP subclass"""

    _ftp = None
    _user = None
    _passwd = None

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
        self._user = user
        self._passwd = passwd
        self._ftp.login(user=user, passwd=passwd)
        self.__force_prot = force_prot
        self.connected = True
        return self

    def __before_all(self):
        self.check_connect()
        if self.__force_prot:
            self._ftp.prot_c()
            self._ftp.prot_p()

    def get(self, path):
        self.__before_all()
        return self._ftp.get_file(path)

    def send(self, path, dest_path=None):
        self.__before_all()
        self._ftp.send_file(path, dest_path)

    def list(self, dir_name):
        self.__before_all()
        return self._ftp.list_directory(dir_name)
