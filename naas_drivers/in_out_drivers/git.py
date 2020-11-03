from naas_drivers.driver import InDriver, OutDriver
from git import Repo
import urllib.parse


class Git(InDriver, OutDriver):
    def connect(self, config):
        """
        Description: This class accepst a JSON configuration and executes respective
        function as per the action specified in the configuration
        """

        self.config = dict(config)

        if [
            "username",
            "password",
            "github_url",
            "branch",
            "target_folder",
            "action",
            "commit_message",
        ] != [i for i in self.config.keys()]:
            print(
                "Configuration missing mandatory parameters (username,password,github_url,branch,target_folder,action)"
            )
            return

        if config.get("action", None) == "clone":
            self.clone(
                dest_dir=self.config.get("target_folder", None),
                branch=self.config.get("branch", None),
                repourl=self.config.get("github_url", None),
                username=self.config.get("username", None),
                password=self.config.get("password", None),
            )
        elif config.get("action", None) == "commit":
            self.commit(
                from_dir=self.config.get("target_folder", None),
                branch=self.config.get("branch", None),
                message=self.config.get("commit_message", None),
            )
        elif config.get("action", None) == "push":
            self.push(
                from_dir=self.config.get("target_folder", None),
                branch=self.config.get("branch", None),
            )
        elif config.get("action", None) == "pull":
            self.pull(
                from_dir=self.config.get("target_folder", None),
                branch=self.config.get("branch", None),
            )
        elif config.get("action", None) == "checkout":
            self.checkout(
                from_dir=self.config.get("target_folder", None),
                branch=self.config.get("branch", None),
            )
        self.connected = True
        return self

    def clone(
        self,
        dest_dir="gitSource",
        branch="master",
        repourl=None,
        username=None,
        password=None,
    ):
        """ Clone the repo (dest_dir='gitSource', branch="master", repourl=None, username=None, password=None)"""
        self.check_connect()
        if repourl[0:3] == "git":
            Repo.clone_from(repourl, dest_dir, branch=branch)
        else:
            urlsplit = repourl.split("//")
            authstring = username + ":" + urllib.parse.quote(password)
            urlsplit[1] = authstring + "@" + urlsplit[1]
            repourl = "//".join(urlsplit)
            Repo.clone_from(repourl, dest_dir, branch=branch)
        print("Clone successful")

    def commit(self, from_dir, branch, message="Auto commit"):
        """ Commit the files commit(from_dir,branch,message="Auto Commit Message")"""
        self.check_connect()
        repo = Repo(from_dir)
        repo.git.execute(["git", "add", "--all"])
        repo.index.commit(message)
        repo.git.clean("-xdf")
        print("Commit successful!")
        return repo.remote()

    def push(self, from_dir, branch="master"):
        """ Push to the specified repository push(self,from_dir,branch="master")"""
        self.check_connect()
        self.checkout(from_dir, branch)
        origin = self.commit(from_dir, branch)
        origin.push()
        print("Push successful")

    def pull(self, from_dir, branch="master"):
        """ Pull from the specified repository pull(from_dir,branch="master")"""
        self.check_connect()
        self.checkout(from_dir, branch)
        origin = self.commit(from_dir, branch)
        origin.pull()
        print("Pull successful!")

    def checkout(self, from_dir, branch="master"):
        """ Checkout to a spcified branch (from_dir, branch="master") """
        self.check_connect()
        repo = Repo(from_dir)
        if repo.active_branch != branch:
            repo.git.clean("-xdf")
            repo.git.checkout(branch)
        print("Checkout successful")
