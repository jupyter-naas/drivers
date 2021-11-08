from naas_drivers.driver import OutDriver
import requests
import pandas as pd
import os
import hashlib


class Taggun(OutDriver):
    _key = None
    _fileName = None
    _filePath = None
    _url = None
    _incognito = None
    _hash = None
    
    data = None

    def connect(self, key, file, mode = 'simple', incognito = False):
        self._key = key
        self.connected = True
        self._url = f'https://api.taggun.io/api/receipt/v1/{mode}/file'
        self._fileName = os.path.basename(file);
        self._filePath = file
        self._incognito = incognito;
        return self
    
    def read(self):
        if not self._fileData:
            self._fileData = open(self._filePath, 'rb')
        return
            
    def hashFile(self):
        hasher = hashlib.sha256()
        hasher.update(copy.copy(self._fileData).read())
        self._hash = hasher.hexdigest()
        return self._hash

    def send(self):
        self.check_connect()
        self.read()
        self.hashFile()
        
        headers = {'apikey': self._key}
        files = {
          'file': (
            self._fileName,
            self._fileData,
            'application/pdf'), # content-type for the file
          # other optional parameters for Taggun API (eg: incognito, refresh, ipAddress, language)
          'incognito': (
            None, #set filename to none for optional parameters
            'false') #value for the parameters
        }
        response = requests.post(self._url, files=files, headers=headers)
        self.data = response.json()
        return self.data
