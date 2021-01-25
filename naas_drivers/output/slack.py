from naas_drivers.driver import OutDriver
from slack import WebClient
from slack.errors import SlackApiError


class Slack(OutDriver):
    client = None

    def connect(self, key):
        self._key = key
        self.client = WebClient(token=self._key)
        self.connected = True
        return self

    def send(self, channel, text):
        self.check_connect()
        try:
            response = self.client.chat_postMessage(channel=channel, text=text)
            assert response["message"]["text"] == text
            print("Message send")
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
