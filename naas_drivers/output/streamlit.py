import os
import subprocess
from pyngrok import ngrok

class NaasStreamlit():
    def __init__(self, path, port=8501):
        self.path = path
        self.run_app()

    def run_app(self, debug=True):
        os.system(f"fuser -n tcp -k {self.port}")
        cmd = f'streamlit run {self.path} --server.port {self.port}'
        with subprocess.Popen(
                [cmd],
                shell=True,
                stdout=subprocess.PIPE,
                bufsize=1,
                universal_newlines=True,
        ) as proc:
            for line in proc.stdout:
                if debug:
                    print(line, end="")


class BaseApp(NaasStreamlit):
    def __init__(self, port=8501):
        super().__init__(port)
        self.port = port
        self._start_server()

    def _start_server(self):
        active_tunnels = ngrok.get_tunnels()
        for tunnel in active_tunnels:
            public_url = tunnel.public_url
            ngrok.disconnect(public_url)
        url = ngrok.connect(addr=self.port, options={"bind_tls": True}, return_ngrok_tunnel=True)
        print(f'Web App can be accessed on: {url.public_url}')



