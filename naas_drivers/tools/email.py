from naas_drivers.driver import OutDriver
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Any, Dict, cast
import pandas as pd
import smtplib
from imap_tools import MailBox, A, AND, MailMessage
from datetime import datetime, date
from typing import Literal, Union


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

    def get_mailbox(self, box=""):
        with MailBox(self.smtp_server).login(self.username, self.password) as mailbox:
            return mailbox.folder.list(box)

    def set_seen(self, uid, status=True):
        with MailBox(self.smtp_server).login(self.username, self.password) as mailbox:
            mailbox.seen(mailbox.fetch(AND(uid=uid)), status)

    def set_flag(self, uid, name, status=True):
        with MailBox(self.smtp_server).login(self.username, self.password) as mailbox:
            flags = name.upper()
            mailbox.flag(mailbox.fetch(AND(uid=uid)), flags, status)

    def status(self, box="INBOX"):
        with MailBox(self.smtp_server).login(self.username, self.password) as mailbox:
            return mailbox.folder.status(box)

    def get(self, box="INBOX", criteria="ALL", limit=None, mark=None):
        emails = []
        mark_seen = False
        mail_filter = "ALL"
        if mark and mark == "seen":
            mark_seen = True
        if criteria and criteria == "seen":
            mail_filter = AND(seen=True)
        elif criteria and criteria == "unseen":
            mail_filter = AND(seen=False)
        with MailBox(self.smtp_server).login(
            self.username, self.password, initial_folder=box
        ) as mailbox:
            emails = []
            for msg in mailbox.fetch(mail_filter, limit=limit, mark_seen=mark_seen):
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

    def get_emails_by_subject(self, subject: str, exact: bool = False):
        """
        Get emails with a given subject. Matches all emails which subjects contain the argument. Case-insensitive.
        Args:
            subject (str): the subject to match
            exact (bool, optional): whether to match the exact subject; defaults to False
        Returns:
            pd.DataFrame
        """
        with MailBox(self.smtp_server).login(self.username, self.password) as mailbox:
            emails = []
            # subject = subject returns case-insensitive substring matches
            for msg in mailbox.fetch(AND(subject=subject)):
                if exact:
                    if msg.subject == subject:
                        parsed = self.__parse_message(msg)
                        emails.append(parsed)
                    else:
                        continue
                else:
                    parsed = self.__parse_message(msg)
                    emails.append(parsed)
            return pd.DataFrame.from_records(emails)

    def get_emails_by_date(
        self,
        date: Union[datetime, date],
        condition=Literal["on", "before", "after", "before or on", "after or on"],
    ):
        """
        Get emails with a given date.
        Args:
            date (datetime): the date to match
            condition (str, optional): the condition to match the date; defaults to "on".
            Possible values are: "on", "before", "after", "before or on", "after or on".
        Returns:
            pd.DataFrame
        """
        with MailBox(self.smtp_server).login(self.username, self.password) as mailbox:
            emails = []
            query = ""
            if condition == "on":
                query = AND(date=date)
            elif condition == "before":
                query = AND(date__lt=date)
            elif condition == "after":
                query = AND(date__gt=date)
            elif condition == "before or on":
                query = AND(date__lte=date)
            elif condition == "after or on":
                query = AND(date__gte=date)

            for msg in mailbox.fetch(query):
                parsed = self.__parse_message(msg)
                emails.append(parsed)
            return pd.DataFrame.from_records(emails)

    def get_emails_by_sender(self, sender: str, exact: bool = False):
        """
        Get emails with a given sender. Matches all emails which senders contain the argument. Case-insensitive.
        Args:
            sender (str): the sender to match (e.g Bob bob@cashstory.com)
            exact (bool, optional): whether to match the exact sender; defaults to False
        Returns:
            pd.DataFrame
        """
        with MailBox(self.smtp_server).login(self.username, self.password) as mailbox:
            emails = []
            # sender = sender returns case-insensitive substring matches
            for msg in mailbox.fetch(AND(from_=sender)):
                if exact:
                    if msg.from_ == sender:
                        parsed = self.__parse_message(msg)
                        emails.append(parsed)
                    else:
                        continue
                else:
                    parsed = self.__parse_message(msg)
                    emails.append(parsed)
            return pd.DataFrame.from_records(emails)

    def get_emails_by_imap_query(self, query: str):
        """
        Get emails with a given IMAP query. You shoud import AND, OR, NOT from imap_query.
        E.g AND(subject="hello", from_=bob@cashstory.com") would return all emails with the
        subject "hello" and the sender bob@cashstory.com.
        Args:
            query (str): the IMAP query to match
        Returns:
            pd.DataFrame
        """
        with MailBox(self.smtp_server).login(self.username, self.password) as mailbox:
            emails = []
            for msg in mailbox.fetch(query):
                parsed = self.__parse_message(msg)
                emails.append(parsed)
            return pd.DataFrame.from_records(emails)

    def __parse_message(self, message: MailMessage):
        parsed = {
            "uid": message.uid,
            "subject": message.subject,
            "from": message.from_values,
            "to": list(message.to_values),
            "cc": list(message.cc_values),
            "bcc": list(message.bcc_values),
            "reply_to": list(message.reply_to_values),
            "date": message.date,
            "text": message.text,
            "html": message.html,
            "flags": message.flags,
            "headers": message.headers,
            "size_rfc822": message.size_rfc822,
            "size": message.size,
            "obj": message.obj,
            "attachments": len(message.attachments),
        }
        return parsed

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
