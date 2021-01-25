from naas_drivers.driver import OutDriver
import pymsteams


class Teams(OutDriver):
    client = None

    def connect(self, key):
        self._key = key
        self.client = pymsteams.connectorcard(self._key)
        self.connected = True
        return self

    def send(self, text):
        self.check_connect()
        self.client.text(text)
        self.client.send()
        print("Message Send")
