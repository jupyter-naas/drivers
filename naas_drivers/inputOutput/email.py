from naas_drivers.driver import OutDriver, InDriver
from email.mime.multipart import MIMEMultipart
from email.header import decode_header
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from typing import Any, Dict, cast
from email.header import Header
from email import encoders
import pandas as pd
import datetime
import smtplib
import imaplib
import email
import re


class Email(InDriver, OutDriver):
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

    def __imap_connect(self):
        imap = imaplib.IMAP4_SSL(self.smtp_server)
        imap.login(self.username, self.password)
        return imap

    def __imap_disconnect(self, imap):
        imap.select()
        imap.close()
        imap.logout()

    def __trimEmail(self, s):
        """
        only necessary for "From" header.
        only necessary for "To" header.
        """
        s = re.sub(r"""\?=["']<""", "?= <", s)
        s = s.replace('"', "")
        s = s.replace("'", "")
        matchObj = re.search(r"\<(.*)\>", s, re.M | re.I)
        if matchObj:
            s = matchObj.group()
            s = s.replace("<", "")
            s = s.replace(">", "")
            return s
        return s

    def get_mailbox(self):
        imap = self.__imap_connect()
        res, boxs = imap.list()
        box_list = []
        for box in boxs:
            list_response_pattern = re.compile(
                r'\((?P<flags>.*?)\) "(?P<delimiter>.*)" (?P<name>.*)'
            )
            folder = box.decode("utf-8")
            flags, delimiter, name = list_response_pattern.match(folder).groups()
            name = name.strip('"')
            box_list.append(name)
        self.__imap_disconnect(imap)
        return box_list

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

    def get(self, box="INBOX", date_limit=None, limit_messages=-1, receiver=None):
        imap = self.__imap_connect()
        status, messages = imap.select(box)
        messages = int(messages[0])
        messages
        emails = []
        total = (
            messages
            if limit_messages == -1 or limit_messages > messages
            else limit_messages
        )
        for i in range(total, 0, -1):
            # fetch the email message by ID
            res, msg = imap.fetch(str(i), "(RFC822)")
            for response in msg:
                if isinstance(response, tuple):
                    # parse a bytes email into a message object
                    msg = email.message_from_bytes(response[1])
                    body = ""
                    content_type = ""
                    # decode the email date
                    date = None
                    date_tuple = email.utils.parsedate_tz(msg["Date"])
                    if date_tuple:
                        date = datetime.datetime.fromtimestamp(
                            email.utils.mktime_tz(date_tuple)
                        )
                    # decode the email subject
                    subject = decode_header(msg["Subject"])[0][0]
                    if isinstance(subject, bytes):
                        # if it's a bytes, decode to str
                        subject = subject.decode()
                    # decode email sender
                    to, encoding = decode_header(msg.get("To"))[0]
                    if to and isinstance(to, bytes):
                        to = to.decode(encoding)
                        to = self.__trimEmail(to)
                    From, encoding = decode_header(msg.get("From"))[0]
                    if From and isinstance(From, bytes):
                        From = From.decode(encoding)
                        From = self.__trimEmail(From)
                    # if the email message is multipart
                    if msg.is_multipart():
                        # iterate over email parts
                        for part in msg.walk():
                            # extract content type of email
                            content_type = part.get_content_type()
                            # get the email body
                            body = part.get_payload(decode=True).decode()
                    else:
                        # extract content type of email
                        content_type = msg.get_content_type()
                        # get the email body
                        body = msg.get_payload(decode=True).decode()
                    if receiver == to:
                        emails.append(
                            {
                                "subject": subject,
                                "from": From,
                                "body": body,
                                "date": date,
                                "content_type": content_type,
                            }
                        )
                    if date_limit and date_limit > date:
                        self.__imap_disconnect(imap)
                        return pd.DataFrame.from_records(emails)
        self.__imap_disconnect(imap)
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
