from naas_drivers.driver import OutDriver
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Any, Dict, cast


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
