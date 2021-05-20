import os
import subprocess
from pyngrok import ngrok


class BaseApp:
    def __init__(self, port=9999):
        self.port = port
        self._start_server()

    def _start_server(self):
        active_tunnels = ngrok.get_tunnels()
        for tunnel in active_tunnels:
            public_url = tunnel.public_url
            ngrok.disconnect(public_url)
        url = ngrok.connect(port=self.port, options={"bind_tls": True})
        print(f'Web App can be accessed on: {url}')


class NaasStreamlit():
    def __init__(self, path, port=9999):
        self.path = path
        self.run_app()
        self._start_server(port=port)

    def _start_server(self,port):
        active_tunnels = ngrok.get_tunnels()
        for tunnel in active_tunnels:
            public_url = tunnel.public_url
            ngrok.disconnect(public_url)
        url = ngrok.connect(port=port, options={"bind_tls": True})
        print(f'Web App can be accessed on: {url}')

    def run_app(self, debug=True,port = 9999):
        os.system(f"fuser -n tcp -k {port}")
        cmd = f'streamlit run {self.path} --server.port {port}'
        print("done1")
        with subprocess.Popen(
                [cmd],
                shell=True,
                stdout=subprocess.PIPE,
                bufsize=1,
                universal_newlines=True,
        ) as proc:
            print("done2")
            for line in proc.stdout:
                if debug:
                    print(line, end="")
