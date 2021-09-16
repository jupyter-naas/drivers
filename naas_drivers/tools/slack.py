from naas_drivers.driver import OutDriver
from slack import WebClient
from slack.errors import SlackApiError
import json


class Slack(OutDriver):
    client = None

    def connect(self, key):
        self._key = key
        self.client = WebClient(token=self._key)
        self.connected = True
        return self

    def __upload_or_link(self, data):
        read_data = data
        if "." in data:
            try:
                read_data = self.__upload_file(data)
            except OSError:
                pass
        return self.__pureimg(read_data)

    def __pureimg(self, data1):
        data1 = '[{"text": "", "image_url": "' + data1 + '"}]'
        data1 = [json.loads(data1[1:-1])]
        return data1

    def __upload_file(self, path):
        response = self.client.files_upload(channel="#theta", file=path)
        return response["file"]["permalink"]

    def send(self, channel, text, image=None):
        self.check_connect()
        try:
            attachments = None
            if image:
                attachments = self.__upload_or_link(image)
            response = self.client.chat_postMessage(
                channel=channel, text=text, attachments=attachments
            )
            assert response["message"]["text"] == text
            print("Message send")
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
