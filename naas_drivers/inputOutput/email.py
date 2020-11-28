from naas_drivers.driver import OutDriver
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Any, Dict, cast
from imap_tools import MailBox, A
import pandas as pd
import smtplib


class Email(OutDriver):
    """
    Connector for sending email from an authenticated email service over SMTP.

    Args:
        - username (str): username for email service
        - password (str): password for email service
        - email_from (str, optional): the email address to send from
        - smtp_server (str, optional): the hostname of the SMTP server;
            defaults to http://smtp.sendgrid.net/
        - smtp_port (int, optional): the port number of the SMTP server; defaults to 465
        - smtp_type (str, optional): either SSL or STARTTLS; defaults to SSL
    """

    def get_mailbox(self, box=""):
        with MailBox(self.smtp_server).login(self.username, self.password) as mailbox:
            return mailbox.folder.list(box)

    def connect(
        self,
        username: str,
        password: str,
        email_from: str = "bob@cashstory.com",
        smtp_server: str = "smtp.sendgrid.net",
        smtp_port: int = 465,
        smtp_type: str = "SSL",
    ):
        self.username = username
        self.password = password
        self.email_from = email_from
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_type = smtp_type
        self.connected = True
        return self

    def get_attachments(self, uid):
        with MailBox(self.smtp_server).login(self.username, self.password) as mailbox:
            attachments = []
            for msg in mailbox.fetch(A(uid=uid)):
                for att in msg.attachments:
                    attachments.append(
                        {
                            "filename": att.filename,
                            "payload": att.payload,
                            "content_id": att.content_id,
                            "content_type": att.content_type,
                            "content_disposition": att.content_disposition,
                            "part": att.part,
                            "size": att.size,
                        }
                    )
        return pd.DataFrame.from_records(attachments)

    def get(self, box="INBOX", limit=None, mark=None):
        emails = []
        mark_seen = False
        if mark and mark == "seen":
            mark_seen = True
        with MailBox(self.smtp_server).login(
            self.username, self.password, initial_folder=box
        ) as mailbox:
            emails = []
            for msg in mailbox.fetch(limit=limit, mark_seen=mark_seen):
                parsed = {
                    "uid": msg.uid,
                    "subject": msg.subject,
                    "from": msg.from_values,
                    "to": list(msg.to_values),
                    "cc": list(msg.cc_values),
                    "bcc": list(msg.bcc_values),
                    "reply_to": list(msg.reply_to_values),
                    "date": msg.date,
                    "text": msg.text,
                    "html": msg.html,
                    "flags": msg.flags,
                    "headers": msg.headers,
                    "size_rfc822": msg.size_rfc822,
                    "size": msg.size,
                    "obj": msg.obj,
                    "attachments": len(msg.attachments),
                }
                emails.append(parsed)
                if mark and mark == "archive":
                    mailbox.delete([msg.uid])
            return pd.DataFrame.from_records(emails)

    def send(
        self,
        email_to: str,
        subject: str,
        content: str = "",
        files: Dict[str, Any] = None,
    ) -> None:
        """
        Method which sends an email.

        Args:
            - email_to (str): the destination email address to send the message to
            - subject (str): the subject of the email
            - content (str, Optional): the contents of the email
            - files (Dict, optional): Dict of attachments to send

        Returns:
            - None
        """
        self.check_connect()
        email_to = cast(str, email_to)

        contents = MIMEMultipart()
        contents.attach(MIMEText(cast(str, content), "plain"))

        contents["Subject"] = Header(subject, "UTF-8")
        contents["From"] = self.email_from
        contents["To"] = email_to
        if files:
            for k, v in files.items():
                p = MIMEBase("application", "octet-stream")
                with open(v, "rb") as f:
                    p.set_payload(f.read())
                encoders.encode_base64(p)
                p.add_header("Content-Disposition", f"attachment; filename= {k}")
                contents.attach(p)

        message = contents.as_string()

        if self.smtp_type == "SSL":
            server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
        elif self.smtp_type == "STARTTLS":
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
        else:
            raise ValueError("Please set smtp_type to SSL or STARTTLS")

        server.login(self.username, self.password)
        try:
            server.sendmail(self.email_from, email_to, message)
        finally:
            server.quit()
            print("email send")
