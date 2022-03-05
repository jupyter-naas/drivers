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
        self.projects = Projects(self.headers)

        # Set connexion to active
        self.connected = True
        return self

class Projects(Github):
    def __init__(self, headers):
        Github.__init__(self)
        self.headers = headers
    
    def get_active_projects_links(self, url):
        """
        Return an dataframe object with 9 columns:
        - PROJECT_NAME           object
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
        page=1
        while True:
            params = {
                "per_page": 100,
                'page': page
            }
            url_link = url + f"?{urlencode(params, safe='(),')}"
            res = requests.get(url_link, headers=self.headers, params=params)

            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                raise(e)
            if len(res.json()) == 0:
                break
            res_json = res.json()

            for idx, project in enumerate(res_json):
                projects_df.loc[idx, 'project_name'] = project.get('name')
                projects_df.loc[idx, 'project_description'] = project.get('body')
                projects_df.loc[idx, 'project_id'] = project.get('number')
                projects_df.loc[idx, 'project_created_by'] = project.get('creator')['login']

                projects_df.loc[idx, 'project_created_date'] = project.get('created_at').strip('Z').split('T')[0]
                projects_df.loc[idx, 'project_created_time'] = project.get('created_at').strip('Z').split('T')[-1]
                projects_df.loc[idx, 'project_updated_date'] = project.get('updated_at').strip('Z').split('T')[0]
                projects_df.loc[idx, 'project_updated_time'] = project.get('updated_at').strip('Z').split('T')[-1]

                projects_df.loc[idx, 'project_columns_url'] = project.get('columns_url')

            page+=1

        projects_df['project_id'] = projects_df.project_id.astype('int')
        return projects_df
    
    def get_comments_from_issues(self, url):
        issue_comments=[]

        if url.find("api.github.com")==-1:
            url = "api.github.com".join(url.split("github.com"))

        comments = requests.get(url, headers=self.headers)
        try:
            comments.raise_for_status()
        except requests.HTTPError as e:
            raise(e)
        if len(comments.json())==0:
            return 'No comments'
        else:
            for comment in comments.json():
                issue_comments.append(comment['body'])
        return issue_comments
    
    def get_issues_from_projects(self, projects_url):
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
        df_projects = self.get_active_projects_links(projects_url)
        df_issues = pd.DataFrame(columns=['issue_status', 'issue_state'])

        ## Gets info from columns present in our roadmap for all active projects ##
        for _, project in df_projects.iterrows():

            columns = requests.get(project['project_columns_url'], headers=self.headers).json()
            issue_status, issue_urls=[],[]
            for column in columns:
                page=1
                while True:
                    params = {
                        'per_page':100,
                        'page':page
                    }
                    card_url = column['cards_url']+ f"?{urlencode(params, safe='(),')}"
                    issues = requests.get(card_url, headers=self.headers)
                    try:
                        issues.raise_for_status()
                    except requests.HTTPError as e:
                        raise(e)
                    if len(issues.json()) == 0:
                        break
                    for issue in issues.json():
                        if issue.get('content_url')!=None:                        
                            issue_urls.append(issue.get('content_url'))
                            issue_status.append(column['name'])
                    page+=1

            df_issues['issue_status'] = issue_status

            for idx, url in enumerate(issue_urls):
                issue = requests.get(url, headers=self.headers)
                try:
                    issue.raise_for_status()
                except requests.HTTPError as e:
                    raise(e)
                issue = issue.json()
                ###### information to be extracted are below #####
                df_issues.loc[idx, 'link_to_the_issue'], df_issues.loc[idx, 'issue_number'] = issue['html_url'], issue['number']
                df_issues.loc[idx, 'issue_title'], df_issues.loc[idx, 'issue_state'] = issue['title'], issue['state']

                labels= []
                for label in issue['labels']:
                    labels.append(label.get('name'))
                df_issues.loc[idx, 'issue_labels'] = ", ".join(labels)

                assigned=[]
                for assignee in issue['assignees']:
                    assigned.append(assignee.get('login'))
                if assigned==[]:
                    df_issues.loc[idx, 'issue_assignees'] = 'None'
                else:
                    df_issues.loc[idx, 'issue_assignees'] = ", ".join(assigned)

                df_issues.loc[idx, 'comments_till_date'] = issue['comments']

                df_issues.loc[idx, 'last_created_date'] = issue.get('created_at').strip('Z').split('T')[0]
                df_issues.loc[idx, 'last_created_time'] = issue.get('created_at').strip('Z').split('T')[-1]
                df_issues.loc[idx, 'last_updated_date'] = issue.get('updated_at').strip('Z').split('T')[0]
                df_issues.loc[idx, 'last_updated_time'] = issue.get('updated_at').strip('Z').split('T')[-1]

                if df_issues.loc[idx, 'issue_status']!='Backlog':
                    date_format = "%Y-%m-%d"
                    delta = datetime.now() - datetime.strptime(df_issues.loc[idx, 'last_updated_date'], date_format)
                    df_issues.loc[idx, 'stale_issue'] = f'No activity since {delta.days} days'
                else:
                    df_issues.loc[idx, 'stale_issue'] = 'None'

                df_issues.loc[idx, 'comments'] = str(self.get_comments_from_issues(issue['comments_url']))

                try:
                    pr = requests.get(issue.get('pull_request')['url'], headers= headers).json()
                    df_issues.loc[idx, 'linked_pr_state'] = pr.get('state')

                    date_format = "%Y-%m-%d"
                    delta = datetime.now() - datetime.strptime(pr.get('updated_at').split('T')[0], date_format)
                    df_issues.loc[idx, 'PR_activity'] = f'No activity since {delta.days} days'

                except:
                    df_issues.loc[idx, 'linked_pr_state'] = 'None'
                    df_issues.loc[idx, 'PR_activity'] = 'None'
                ##################################################

            df_issues['project_id'] = [df_projects.project_id.values[0]]*df_issues.shape[0]
            df_issues['project_name'] = [df_projects.project_name.values[0]]*df_issues.shape[0]
            df_issues['issue_number'] = df_issues['issue_number'].apply(lambda x: int(x))
            df_issues['comments_till_date'] = df_issues['comments_till_date'].apply(lambda x: int(x))

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
        headers['Accept'] = 'application/vnd.github.v3.star+json'
        
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
                raise(e)
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