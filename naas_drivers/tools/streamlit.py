import os
import subprocess
from pyngrok import ngrok


class BaseApp:
    def _start_server(self, port):
        active_tunnels = ngrok.get_tunnels()
        for tunnel in active_tunnels:
            public_url = tunnel.public_url
            ngrok.disconnect(public_url)
        url = ngrok.connect(addr=port, options={"bind_tls": True})
        print(f"Web App can be accessed on: {url}")


class Streamlit(BaseApp):
    """
    Naas Streamlit app
    """

    def add(self, path="app.py", port=9999, debug=True):
        self.path = path
        self.port = port
        self._start_server(port)
        os.system(f"fuser -n tcp -k {self.port}")
        cmd = f"streamlit run {self.path} --server.port {self.port}"
        with subprocess.Popen(
            [cmd],
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            universal_newlines=True,
        ) as proc:
            print(cmd)
            print(proc)
            for line in proc.stdout.readlines():
                if debug:
                    print(line, end="")
            for line in proc.stderr.readlines():
                if debug:
                    print(line, end="")
