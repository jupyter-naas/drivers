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
        cmd = f"streamlit run {self.path} --server.port {self.port}  --logger.level debug"
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
            username = os.environ.get('JUPYTERHUB_USER', None)
            if username:
                print(f"\n🎉 Streamlit application is accessible here: 'https://app.naas.ai/user/{username}/proxy/{port}/'")
                
            while proc.returncode is None:
                    line = proc.stdout.readline()
                    if debug:
                        print(line.rstrip())
                    line = proc.stderr.readline()
                    if debug:
                        print(line.rstrip())
            
