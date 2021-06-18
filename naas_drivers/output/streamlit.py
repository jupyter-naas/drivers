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
        url = ngrok.connect(addr=self.port, options={"bind_tls": True})
        print(f"Web App can be accessed on: {url}")


class NaasStreamlit(BaseApp):
    """
    Naas Streamlit app
    """

    def __init__(self, path="app.py", port=9999):
        super().__init__(port)
        self.path = path
        self.run_app()

    def run_app(self, debug=True):
        os.system(f"fuser -n tcp -k {self.port}")
        cmd = f"streamlit run {self.path} --server.port {self.port}"
        with subprocess.Popen(
            [cmd],
            shell=True,
            stdout=subprocess.PIPE,
            bufsize=1,
            universal_newlines=True,
        ) as proc:
            print(cmd)
            print(proc)
            for line in proc.stdout.readlines():
                if debug:
                    print(line, end="")
