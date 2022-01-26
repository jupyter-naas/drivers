from naas_drivers.driver import OutDriver
import hashlib
import os
import requests


class Taggun(OutDriver):
    """
    Naas Driver for Taggun.io

    Taggun offers affordable Receipt/invoice OCR,
        Input: a pdf or image of a receipt
        Output: a dict containing parsed data from the image, and a corresponding
            confidence level for the extraction
    """

    _key = None
    _fileName = None
    _filePath = None
    _url = None
    _incognito = None
    _hash = None

    data = None

    def connect(self, key, mode="simple", incognito=False):
        """
        Initialize all the necessary elements to make an api request

        Args:
            key (string): Taggun API key
            mode (string): Either simple or verbose. default simple
            incognite (bool): If true, taggun will not store a copy of the
                uploaded file for reinforcement learning. default false
        """
        self._key = key
        self.connected = True
        self._url = f"https://api.taggun.io/api/receipt/v1/{mode}/file"
        self._incognito = incognito
        return self

    def hash_file(self, file):
        """
        Create a hash of the file to be sent. Taggun allows user-submitted data
        for reinforcement learning. This requires a unique identifier for the file.
        Instead of generating a UUID, which would need to be stored, use a hash
        of the file itself as a unique identifier

        Returns:
            hash of the file
        """
        with open(file, "rb") as f:
            self._hash = hashlib.sha256(f.read()).hexdigest()
        return self._hash

    def send(self, file):
        """
        Dispatch the api request to Taggun and receive parsed data

        Args:
        file (string): Path to the file being sent for parsing

        Returns:
            parsed data as a dict from Taggun
        """
        self._fileName = os.path.basename(file)
        self._filePath = file
        self.check_connect()
        self.hash_file(self._filePath)

        with open(self._filePath, "rb") as f:
            headers = {"apikey": self._key}
            files = {
                "file": (
                    self._fileName,
                    f,
                    "application/pdf",
                ),  # content-type for the file
                # other optional parameters for Taggun API (eg: incognito, refresh, ipAddress, language)
                "incognito": (
                    None,  # set filename to none for optional parameters
                    self._incognito,
                ),  # value for the parameters
                "referenceID": (
                    None,  # set filename to none for optional parameters
                    self._hash,  # value for the parameters
                ),
            }
            response = requests.post(self._url, files=files, headers=headers)
            self.data = response.json()
        return self.data
