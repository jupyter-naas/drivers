import base64
import errno
import json
import os

class Secret():
    production_path = os.path.join(os.path.sep, 'home', os.environ.get('NB_USER', 'ftp'), 'ftp', '.brain')
    json_secrets = 'secrets.json'
    json_secrets_path= None
    
    def __init__(self, production_path=None, json_secrets=None, mode='prod'):
        self.json_secrets = json_secrets if json_secrets else self.json_secrets
        if production_path is not None:
            self.production_path = production_path
        self.json_secrets_path = os.path.join(self.production_path, self.json_secrets)
        if not os.path.exists(os.path.dirname(self.json_secrets_path)):
            try:
                os.makedirs(os.path.dirname(self.json_secrets_path))
            except OSError as exc: # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

    def __set_secret(self, newSecret):
        secret_data = json.dumps(newSecret, sort_keys=True, indent=4)
        with open(self.json_secrets_path, 'w+') as f:
            f.write(secret_data)
            f.close()

    def __get_all(self):
        secret_data = []
        try:
            with open(self.json_secrets_path, 'r') as f:
                secret_data = json.load(f)
                f.close()
        except:
            secret_data = [] 
        return secret_data
    
    def list(self):
        all_secret = self.__get_all()
        all_keys = []
        for item in all_secret:
            all_keys.append(item['name'])
        return all_keys

    def add(self, name=None, secret=None):
        new_obj = []
        obj = {}
        json_data = self.__get_all()
        replaced = False
        message_bytes = secret.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        secret_base64 = base64_bytes.decode('ascii')
        for item in json_data:
            if name == item['name']:
                obj = {'name': name, 'secret': secret_base64}
                new_obj.append(obj)
                print('Edited =>>', obj)
                replaced = True
            else:
                new_obj.append(item)
        if replaced is False:
            obj = {'name': name, 'secret': secret_base64}
            new_obj.append(obj)
            print('Added =>>', obj)
        self.__set_secret(new_obj)
        return obj
        
    def get(self, name=None, default_value=None):
        all_secret = self.__get_all()
        secret_item = None
        for item in all_secret:
            if name == item['name']:
                secret_item = item
                break
        if secret_item is not None:
            secret_base64 = secret_item.get('secret', None)
            if secret_base64 is not None:
                secret = base64.b64decode(secret_base64)
                return secret.decode("ascii") 
        return default_value

    def delete(self, name=None):
        new_obj = []
        found = False
        json_data = self.__get_all()
        for item in json_data:
            if name != item['name']:
                new_obj.append(item)
            else:
                found = True
        if (len(json_data) != len(new_obj)):
            self.__set_secret(new_obj)
        if found:
            print('Deleted =>>', name)
        else:
            print('Not found =>>', name)
        return None


