import pandas as pd
import requests
import pydash as _pd
from urllib.parse import urlencode
from datetime import datetime


class Github:
    @staticmethod
    def get_repository_url(url):
        return url.split("https://github.com/")[-1]

    def connect(self, token: str):
        # Init connect
        self.token = token

        # Init headers
        self.headers = {"Authorization": f"token {self.token}"}

        # Init end point
        self.repos = Repositories(self.headers)
        self.users = Users(self.headers)
        self.teams = Teams(self.headers)
        self.projects = Projects(self.headers)

        # Set connexion to active
        self.connected = True
        return self


class Users(Github):
    def __init__(self, headers):
        Github.__init__(self)
        self.headers = headers

    def get_profile(self, html_url, url=None):
        """
        Return a dataframe object with 20 columns:
        - LOGIN               object
        - ID                  int64
        - NODE_ID             object
        - GRAVATAR_ID         object
        - TYPE                object
        - SITE_ADMIN          bool
        - NAME                object
        - COMPANY             object
        - BLOG                object
        - LOCATION            object
        - EMAIL               object
        - HIREABLE            object
        - BIO                 object
        - TWITTER_USERNAME    object
        - PUBLIC_REPOS        int64
        - PUBLIC_GISTS        int64
        - FOLLOWERS           int64
        - FOLLOWING           int64
        - CREATED_AT          object
        - UPDATED_AT          object

        Parameters
        ----------
        html_url: str:
            User profile url from Github.
            Example : "https://github.com/SanjuEpic"
        """
        if url is None:
            user = html_url.split("github.com/")[-1].split("/")[0]
            url = f"https://api.github.com/users/{user}"

        res = requests.get(url, headers=self.headers)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            raise (e)
        res_json = res.json()

        # Dataframe
        df = pd.DataFrame([res_json])
        for col in df.columns:
            if col.endswith("url"):
                df = df.drop(col, axis=1)
            if col.endswith("_at"):
                df[col] = df[col].str.replace("T", " ").str.replace("Z", " ")
        return df


class Teams(Github):
    def __init__(self, headers):
        Github.__init__(self)
        self.headers = headers

    def get_profiles(self, url):
        """
        Return an dataframe object with 14 columns:
        - TEAM              object
        - SLUG              object
        - TEAM_DESCRIPTION  object
        - MEMBER_PROFILE    object
        - GITHUB            object
        - NAME              object
        - EMAIL             object
        - LOCATION          object
        - ORGANIZATION      object
        - BIO               object
        - LOGIN_NAME        object
        - TWITTER           object
        - CREATED_AT        object
        - UPDATED_AT        object

        Parameters
        ----------
        url: str:
            teams url from Github.
            Example : "https://github.com/orgs/jupyter-naas/teams"
        """
        # Traverses through multiple teams and all member profiles within each team
        org = url.split("https://github.com/orgs/")[-1].split("/")[0]

        member_profiles, teams, slugs, team_descriptions = [], [], [], []
        data = pd.DataFrame(
            columns=["TEAM", "SLUG", "TEAM_DESCRIPTION", "member_profile", "GITHUB"]
        )
        page = 1

        while True:
            params = {
                "state": "open",
                "per_page": "100",
                "page": page,
            }
            url = f"https://api.github.com/orgs/{org}/teams?{urlencode(params, safe='(),')}"
            res = requests.get(url, headers=self.headers)
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                raise (e)
            res_json = res.json()

            if len(res_json) == 0:
                break

            members_details = []
            for team_info in res_json:
                members_details.append(
                    (
                        team_info["name"],
                        team_info["slug"],
                        team_info["description"],
                        team_info["members_url"].strip("{/member}"),
                    )
                )

            for info in members_details:
                page_number = 1
                while True:
                    members_params = {
                        "state": "open",
                        "per_page": "100",
                        "page": page_number,
                    }

                    url = f"{info[3]}?{urlencode(members_params, safe='(),')}"
                    members = requests.get(
                        url, headers=self.headers, params=members_params
                    )

                    try:
                        members.raise_for_status()
                    except requests.HTTPError as e:
                        raise (e)
                    members_json = members.json()

                    if len(members_json) == 0:
                        break

                    for member in members_json:
                        member_profiles.append(member["url"])
                        teams.append(info[0])
                        slugs.append(info[1])
                        team_descriptions.append(info[2])

                    page_number += 1

            page += 1

        data["TEAM"], data["SLUG"], data["TEAM_DESCRIPTION"], data["member_profile"] = (
            teams,
            slugs,
            team_descriptions,
            member_profiles,
        )
        data["GITHUB"] = org

        for idx, profile in enumerate(data["member_profile"]):
            details = requests.get(profile, headers=self.headers, params=params).json()
            data.loc[idx, "NAME"], data.loc[idx, "EMAIL"], data.loc[idx, "LOCATION"] = (
                details["name"],
                details["email"],
                details["location"],
            )
            (
                data.loc[idx, "ORGANIZATION"],
                data.loc[idx, "BIO"],
                data.loc[idx, "LOGIN_NAME"],
            ) = (details["company"], details["bio"], details["login"])
            data.loc[idx, "TWITTER"], data.loc[idx, "CREATED_AT"] = (
                details["twitter_username"],
                details["created_at"],
            )
            data.loc[idx, "UPDATED_AT"] = details["updated_at"]
        return data


class Projects(Github):
    def __init__(self, headers):
        Github.__init__(self)
        self.headers = headers

    def get(self, url):
        """
        Return an dataframe object with 9 columns:
        - PROJECT_NAME            object
        - PROJECT_DESCRIPTION     object
        - PROJECT_ID              int64
        - PROJECT_CREATED_BY      object
        - PROJECT_CREATED_DATE    object
        - PROJECT_CREATED_TIME    object
        - PROJECT_UPDATED_DATE    object
        - PROJECT_UPDATED_TIME    object
        - PROJECT_COLUMNS_URL     object

        Parameters
        ----------
        url: str:
            Projects url from Github.
            Example : "https://github.com/orgs/jupyter-naas/projects"
        """
        projects_df = pd.DataFrame()
        url = "api.github.com".join(url.split("github.com"))
        page = 1
        while True:
            params = {"per_page": 100, "page": page}
            url_link = url + f"?{urlencode(params, safe='(),')}"
            res = requests.get(url_link, headers=self.headers, params=params)

            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                raise (e)
            if len(res.json()) == 0:
                break
            res_json = res.json()

            for idx, project in enumerate(res_json):
                projects_df.loc[idx, "project_name"] = project.get("name")
                projects_df.loc[idx, "project_description"] = project.get("body")
                projects_df.loc[idx, "project_id"] = project.get("number")
                projects_df.loc[idx, "project_created_by"] = project.get("creator")[
                    "login"
                ]
                projects_df.loc[idx, "project_created_date"] = (
                    project.get("created_at").strip("Z").split("T")[0]
                )
                projects_df.loc[idx, "project_created_time"] = (
                    project.get("created_at").strip("Z").split("T")[-1]
                )
                projects_df.loc[idx, "project_updated_date"] = (
                    project.get("updated_at").strip("Z").split("T")[0]
                )
                projects_df.loc[idx, "project_updated_time"] = (
                    project.get("updated_at").strip("Z").split("T")[-1]
                )
                projects_df.loc[idx, "project_columns_url"] = project.get("columns_url")

            page += 1

        projects_df["project_id"] = projects_df.project_id.astype("int")
        return projects_df

    def get_comments_from_issues(self, url):
        """
        Returns a list of comments to a particular issue

        Parameters
        ----------
        issue comments url: str
        Example: https://github.com/jupyter-naas/awesome-notebooks/issues/359/comments
        """
        issue_comments = []

        if url.find("api.github.com") == -1:
            url = "api.github.com/repos".join(url.split("github.com"))

        comments = requests.get(url, headers=self.headers)
        try:
            comments.raise_for_status()
        except requests.HTTPError as e:
            raise (e)
        if len(comments.json()) == 0:
            return "No comments"
        else:
            for comment in comments.json():
                issue_comments.append(comment["body"])
        return issue_comments

    def get_issues(self, projects_url):
        """
        Return an dataframe object with 18 columns:
        - ISSUE_STATUS          object
        - ISSUE_STATE           object
        - LINK_TO_THE_ISSUE     object
        - ISSUE_NUMBER          int64
        - ISSUE_TITLE           object
        - ISSUE_LABELS          object
        - ISSUE_ASSIGNEES       object
        - COMMENTS_TILL_DATE    int64
        - LAST_CREATED__DATE    object
        - LAST_CREATED__TIME    object
        - LAST_UPDATED_DATE     object
        - LAST_UPDATED_TIME     object
        - STALE_ISSUE           object
        - COMMENTS              object
        - LINKED_PR_STATE       object
        - PR_ACTIVITY           object
        - PROJECT_ID            int64
        - PROJECT_NAME          object

        Parameters
        ----------
        projects_url: str:
            Projects url from Github.
            Example : "https://github.com/orgs/jupyter-naas/projects"
        """
        df_projects = self.get(projects_url)
        df_issues = pd.DataFrame(columns=["issue_status", "issue_state"])

        # Gets info from columns present in our roadmap for all active projects
        for _, project in df_projects.iterrows():

            columns = requests.get(
                project["project_columns_url"], headers=self.headers
            ).json()
            issue_status, issue_urls = [], []
            for column in columns:
                page = 1
                while True:
                    params = {"per_page": 100, "page": page}
                    card_url = column["cards_url"] + f"?{urlencode(params, safe='(),')}"
                    issues = requests.get(card_url, headers=self.headers)
                    try:
                        issues.raise_for_status()
                    except requests.HTTPError as e:
                        raise (e)
                    if len(issues.json()) == 0:
                        break
                    for issue in issues.json():
                        if issue.get("content_url") is not None:
                            issue_urls.append(issue.get("content_url"))
                            issue_status.append(column["name"])
                    page += 1

            df_issues["issue_status"] = issue_status

            for idx, url in enumerate(issue_urls):
                issue = requests.get(url, headers=self.headers)
                try:
                    issue.raise_for_status()
                except requests.HTTPError as e:
                    raise (e)
                issue = issue.json()
                # information to be extracted are below
                (
                    df_issues.loc[idx, "link_to_the_issue"],
                    df_issues.loc[idx, "issue_number"],
                ) = (issue["html_url"], issue["number"])
                df_issues.loc[idx, "issue_title"], df_issues.loc[idx, "issue_state"] = (
                    issue["title"],
                    issue["state"],
                )

                labels = []
                for label in issue["labels"]:
                    labels.append(label.get("name"))
                df_issues.loc[idx, "issue_labels"] = ", ".join(labels)

                assigned = []
                for assignee in issue["assignees"]:
                    assigned.append(assignee.get("login"))
                if assigned == []:
                    df_issues.loc[idx, "issue_assignees"] = "None"
                else:
                    df_issues.loc[idx, "issue_assignees"] = ", ".join(assigned)

                df_issues.loc[idx, "comments_till_date"] = issue["comments"]

                df_issues.loc[idx, "last_created_date"] = (
                    issue.get("created_at").strip("Z").split("T")[0]
                )
                df_issues.loc[idx, "last_created_time"] = (
                    issue.get("created_at").strip("Z").split("T")[-1]
                )
                df_issues.loc[idx, "last_updated_date"] = (
                    issue.get("updated_at").strip("Z").split("T")[0]
                )
                df_issues.loc[idx, "last_updated_time"] = (
                    issue.get("updated_at").strip("Z").split("T")[-1]
                )

                if df_issues.loc[idx, "issue_status"] != "Backlog":
                    date_format = "%Y-%m-%d"
                    delta = datetime.now() - datetime.strptime(
                        df_issues.loc[idx, "last_updated_date"], date_format
                    )
                    df_issues.loc[
                        idx, "stale_issue"
                    ] = f"No activity since {delta.days} days"
                else:
                    df_issues.loc[idx, "stale_issue"] = "None"

                df_issues.loc[idx, "comments"] = str(
                    self.get_comments_from_issues(issue["comments_url"])
                )

                try:
                    pr = requests.get(
                        issue.get("pull_request")["url"], headers=self.headers
                    ).json()
                    df_issues.loc[idx, "linked_pr_state"] = pr.get("state")

                    date_format = "%Y-%m-%d"
                    delta = datetime.now() - datetime.strptime(
                        pr.get("updated_at").split("T")[0], date_format
                    )
                    df_issues.loc[
                        idx, "PR_activity"
                    ] = f"No activity since {delta.days} days"

                except Exception:
                    df_issues.loc[idx, "linked_pr_state"] = "None"
                    df_issues.loc[idx, "PR_activity"] = "None"

            df_issues["project_id"] = [
                df_projects.project_id.values[0]
            ] * df_issues.shape[0]
            df_issues["project_name"] = [
                df_projects.project_name.values[0]
            ] * df_issues.shape[0]
            df_issues["issue_number"] = df_issues["issue_number"].apply(
                lambda x: int(x)
            )
            df_issues["comments_till_date"] = df_issues["comments_till_date"].apply(
                lambda x: int(x)
            )

        return df_issues


class Repositories(Github):
    def __init__(self, headers):
        Github.__init__(self)
        self.headers = headers

    def get_commits(self, url):
        """
        Return an dataframe object with 11 columns:
        - ID                   object
        - MESSAGE              object
        - AUTHOR_DATE          date
        - AUTHOR_NAME          object
        - AUTHOR_EMAIL         object
        - COMMITTER_DATE       date
        - COMMITTER_NAME       object
        - COMMITTER_EMAIL      object
        - COMMENTS_COUNT       int
        - VERIFICATION_REASON  object
        - VERIFICATION_STATUS  object

        Parameters
        ----------
        repository: str:
            Repository url from Github.
            Example : "https://github.com/jupyter-naas/awesome-notebooks"
        """
        # Get organisation and repository from url
        repository = Github.get_repository_url(url)

        # Get commits
        commits = []
        page = 1
        while True:
            params = {
                "state": "open",
                "per_page": "100",
                "page": page,
            }
            url = f"https://api.github.com/repos/{repository}/commits?{urlencode(params, safe='(),')}"
            res = requests.get(url, headers=self.headers)
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                raise (e)
            res_json = res.json()

            if len(res_json) == 0:
                break
            for r in res_json:
                commit = {
                    "ID": _pd.get(r, "sha"),
                    "MESSAGE": _pd.get(r, "commit.message"),
                    "AUTHOR_DATE": _pd.get(r, "commit.author.date")
                    .replace("T", " ")
                    .replace("Z", ""),
                    "AUTHOR_NAME": _pd.get(r, "commit.author.name"),
                    "AUTHOR_EMAIL": _pd.get(r, "commit.author.email"),
                    "COMMITTER_DATE": _pd.get(r, "commit.committer.date")
                    .replace("T", " ")
                    .replace("Z", ""),
                    "COMMITTER_NAME": _pd.get(r, "commit.committer.name"),
                    "COMMITTER_EMAIL": _pd.get(r, "commit.committer.email"),
                    "COMMENTS_COUNT": _pd.get(r, "commit.comment_count"),
                    "VERIFICATION_REASON": _pd.get(r, "commit.verification.reason"),
                    "VERIFICATION_STATUS": _pd.get(r, "commit.verification.verified"),
                }
                commits.append(commit)
            page += 1
        # Return dataframe
        df = pd.DataFrame(commits)
        df["AUTHOR_DATE"] = pd.to_datetime(df["AUTHOR_DATE"])
        df["COMMITTER_DATE"] = pd.to_datetime(df["COMMITTER_DATE"])
        return df

    def get_stargazers(self, url):
        """
        Return an dataframe object with 6 columns:
        - LOGIN       object
        - ID          int64
        - URL         object
        - TYPE        object
        - SITE_ADMIN  bool
        - STARRED_AT  object

        Parameters
        ----------
        repository: str:
            Repository url from Github.
            Example : "https://github.com/jupyter-naas/awesome-notebooks"
        """
        # Get organisation and repository from url
        repository = Github.get_repository_url(url)

        # Custom headers
        headers = self.headers
        headers["Accept"] = "application/vnd.github.v3.star+json"

        df = pd.DataFrame()
        page = 1
        while True:
            params = {
                "per_page": "100",
                "page": page,
            }
            url = f"https://api.github.com/repos/{repository}/stargazers?{urlencode(params, safe='(),')}"
            res = requests.get(url, headers=headers)
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                raise (e)
            res_json = res.json()

            if len(res_json) == 0:
                break
            for json in res_json:
                starred_at = json.get("starred_at")
                user = json.get("user")
                tmp = pd.DataFrame([user])
                tmp["starred_at"] = starred_at
                df = pd.concat([df, tmp], axis=0)
            page += 1

        # Cleaning
        for col in df.columns:
            if col.endswith("_url") or col.endswith("_id"):
                df = df.drop(col, axis=1)
            if col.endswith("_at"):
                df[col] = df[col].str.replace("T", " ").str.replace("Z", " ")
        df.columns = df.columns.str.upper()
        return df

    def get_comments_from_issues(self, url):
        """
        Returns a list of comments to a particular issue

        Parameters
        ----------
        issue comments url: str
        Example: https://github.com/jupyter-naas/awesome-notebooks/issues/359/comments
        """
        issue_comments = []

        if url.find("api.github.com") == -1:
            url = "api.github.com".join(url.split("github.com"))

        comments = requests.get(url, headers=self.headers)
        try:
            comments.raise_for_status()
        except requests.HTTPError as e:
            raise (e)
        if len(comments.json()) == 0:
            return "No comments"
        else:
            for comment in comments.json():
                issue_comments.append(comment["body"])
        return issue_comments

    def get_issues(self, url):
        """
        Return an dataframe object with 15 columns:
        - LINK_TO_THE_ISSUE      object
        - ISSUE_NUMBER           int64
        - ISSUE_TITLE            object
        - ISSUE_STATE            object
        - ISSUE_ID               int64
        - ISSUE_LABELS           object
        - ISSUE_ASSIGNEES        object
        - COMMENTS_TILL_DATE     int64
        - LAST_CREATED_DATE      object
        - LAST_CREATED_TIME      object
        - LAST_UPDATED_DATE      object
        - LAST_UPDATED_TIME      object
        - COMMENTS               object
        - LINKED_PR_STATE        object
        - PR_ACTIVITY            object

        Parameters
        ----------
        repository: str:
            Repository url from Github.
            Example : "https://github.com/jupyter-naas/awesome-notebooks"
        """
        # Get organisation and repository from url
        repository = Github.get_repository_url(url)

        df = pd.DataFrame()
        page, idx = 1, 0
        while True:
            params = {
                "per_page": "100",
                "page": page,
            }
            url = f"https://api.github.com/repos/{repository}/issues?{urlencode(params, safe='(),')}"
            res = requests.get(url, headers=self.headers)
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                raise (e)
            res_json = res.json()
            if len(res_json) == 0:
                break

            for issue in res_json:
                df.loc[idx, "link_to_the_issue"], df.loc[idx, "issue_number"] = (
                    issue["html_url"],
                    issue["number"],
                )
                df.loc[idx, "issue_title"], df.loc[idx, "issue_state"] = (
                    issue["title"],
                    issue["state"],
                )
                df.loc[idx, "issue_id"] = issue["id"]
                labels = []
                for label in issue["labels"]:
                    labels.append(label.get("name"))
                if labels == []:
                    df.loc[idx, "issue_labels"] = "None"
                else:
                    df.loc[idx, "issue_labels"] = ", ".join(labels)

                assigned = []
                for assignee in issue["assignees"]:
                    assigned.append(assignee.get("login"))
                if assigned == []:
                    df.loc[idx, "issue_assignees"] = "None"
                else:
                    df.loc[idx, "issue_assignees"] = ", ".join(assigned)

                df.loc[idx, "comments_till_date"] = issue["comments"]

                df.loc[idx, "last_created_date"] = (
                    issue.get("created_at").strip("Z").split("T")[0]
                )
                df.loc[idx, "last_created_time"] = (
                    issue.get("created_at").strip("Z").split("T")[-1]
                )
                df.loc[idx, "last_updated_date"] = (
                    issue.get("updated_at").strip("Z").split("T")[0]
                )
                df.loc[idx, "last_updated_time"] = (
                    issue.get("updated_at").strip("Z").split("T")[-1]
                )

                df.loc[idx, "comments"] = str(
                    self.get_comments_from_issues(issue["comments_url"])
                )

                try:
                    pr = requests.get(
                        issue.get("pull_request")["url"], headers=self.headers
                    ).json()
                    df.loc[idx, "linked_pr_state"] = pr.get("state")

                    date_format = "%Y-%m-%d"
                    delta = datetime.now() - datetime.strptime(
                        df.loc[idx, "last_updated_date"], date_format
                    )
                    df.loc[idx, "PR_activity"] = f"No activity since {delta.days} days"

                except Exception:
                    df.loc[idx, "linked_pr_state"] = "None"
                    df.loc[idx, "PR_activity"] = "None"

                idx += 1

            page += 1

        df["issue_id"] = df.issue_id.astype("int")
        df["comments_till_date"] = df.comments_till_date.astype("int")
        df["issue_number"] = df.issue_number.astype("int")

        return df

    def get_pulls(self, url):
        """
        Return an dataframe object with 15 columns:
        - ID                      int64
        - ISSUE_URL               object
        - PR_NUMBER               int64
        - PR_STATE                object
        - TITLE                   object
        - FIRST_CREATED_DATE      object
        - FIRST_CREATED_TIME      object
        - LAST_UPDATED_DATE       object
        - LAST_UPDATED_TIME       object
        - COMMITS_URL             object
        - REVIEW_COMMENTS_URL     object
        - ISSUE_COMMENTS_URL      object
        - ASSIGNEES               object
        - REQUESTED_REVIEWERS     object
        - PR_ACTIVITY             object

        Parameters
        ----------
        repository: str:
            Repository url from Github.
            Example : "https://github.com/jupyter-naas/awesome-notebooks"
        """
        # Get organisation and repository from url
        repository = Github.get_repository_url(url)

        df = pd.DataFrame()
        page = 1
        while True:
            params = {
                "per_page": "100",
                "page": page,
            }
            url = f"https://api.github.com/repos/{repository}/pulls?{urlencode(params, safe='(),')}"
            res = requests.get(url, headers=self.headers)
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                raise (e)
            res_json = res.json()
            if len(res_json) == 0:
                break

            for idx, r in enumerate(res_json):
                if r.get("state") == "open":
                    df.loc[idx, "id"] = r.get("id")
                    df.loc[idx, "issue_url"] = r.get("issue_url")
                    df.loc[idx, "PR_number"] = r.get("number")
                    df.loc[idx, "PR_state"] = "open"
                    df.loc[idx, "Title"] = r.get("title")

                    df.loc[idx, "first_created_date"] = (
                        r.get("created_at").strip("Z").split("T")[0]
                    )
                    df.loc[idx, "first_created_time"] = (
                        r.get("created_at").strip("Z").split("T")[-1]
                    )
                    df.loc[idx, "last_updated_date"] = (
                        r.get("updated_at").strip("Z").split("T")[0]
                    )
                    df.loc[idx, "last_updated_time"] = (
                        r.get("updated_at").strip("Z").split("T")[-1]
                    )

                    df.loc[idx, "commits_url"] = r.get("commits_url")
                    df.loc[idx, "review_comments_url"] = r.get("review_comments_url")
                    df.loc[idx, "issue_comments_url"] = r.get("comments_url")

                    assignees_lst, reviewers_lst = [], []
                    for assignee in r.get("assignees"):
                        assignees_lst.append(assignee.get("login"))
                    for reviewer in r.get("requested_reviewers"):
                        reviewers_lst.append(reviewer.get("login"))

                    if assignees_lst == []:
                        df.loc[idx, "assignees"] = "None"
                    elif assignees_lst:
                        df.loc[idx, "assignees"] = ", ".join(assignees_lst)

                    if reviewers_lst == []:
                        df.loc[idx, "requested_reviewers"] = "None"
                    elif reviewers_lst:
                        df.loc[idx, "requested_reviewers"] = ", ".join(reviewers_lst)

                    date_format = "%Y-%m-%d"
                    delta = datetime.now() - datetime.strptime(
                        df.loc[idx, "last_updated_date"], date_format
                    )
                    df.loc[idx, "PR_activity"] = f"No activity since {delta.days} days"

                df["PR_number"] = df.PR_number.astype("int")
                df.id = df.id.astype("int")

            page += 1

        return df
