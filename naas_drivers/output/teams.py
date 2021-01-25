from naas_drivers.driver import OutDriver
import pymsteams


class Teams(OutDriver):
    client = None

    def connect(self, key):
        self._key = key
        self.client = pymsteams.connectorcard(self._key)
        self.connected = True
        return self

    def send(self, text, title=None, image=None, imageTitle=None):
        self.check_connect()
        self.client.text(text)
        if title:
            self.client.title(title)
        if image:
            myMessageSection = pymsteams.cardsection()
            myMessageSection.addImage(image, ititle=imageTitle)
            self.client.addSection(myMessageSection)
        self.client.send()
        print("Message Send")
