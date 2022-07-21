import os
import subprocess

class Streamlit():
    """
    Naas Streamlit app
    """

    def add(self, path="app.py", port=9999, debug=True):
        self.path = path
        self.port = port
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
