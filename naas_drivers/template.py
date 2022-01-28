class Driver():
    @staticmethod
    def connect(self, token: str):
        # Init connect
        self.token = token
        
        # Init headers
        self.headers = {
            'Authorization': f'token {self.token}'
        }

        # Init end point
        self.endpoint = Endpoint(self.headers)

        # Set connexion to active
        self.connected = True
        return self


class Endpoint(Driver):
    def __init__(self, headers):
        Driver.__init__(self)
        self.headers = headers
        
    def get(self):
        return 'Test'