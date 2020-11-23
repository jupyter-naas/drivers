from naas_drivers.driver import InDriver, OutDriver
import pandas as pd
import requests
import datetime
import os

class HSCRUD:
    # class HSCRUD(CRUD):
    def __init__(self, base_url, auth, req_headers, params):
        self.req_headers = req_headers
        self.params = params
        self.base_url = base_url
        self.model_name = self.base_url.split("/")[-1]
        
    def __values_format(self, data):
        for key, value in data['properties'].items():
            print(value)
            print(key)
            if key == 'filename':
                value = string.capwords(str(value).strip().replace('-',' ')).replace(' ','-')
            elif key == 'lastname':
                value = str(value).strip().upper()
            elif key == 'phone':
                if value == None:
                    value = ""
                else:
                    value = str(value).strip().replace(' ','').replace('.','')
            elif key == 'closedate':
                check_closedate = True
                # Check if close date is in correct format
                if not value is None:
                    if len(value) == 0:
                        print(f"Close date '{value}' is empty. Please fill in the function.")
                        check_closedate = False
                    else:
                        try:
                            value = datetime.datetime.strptime(value, "%d/%m/%Y")
                            value = str(int(closedate.timestamp()))+"000"
                        except:
                            check_closedate = False
                            print(f"Close date '{value}' is not in the correct format.\n"
                                   "Please change it to %d/%m/%Y.")
            else:
                if value != None:
                    value = value.strip()

    def __get_by_page(self, params):
        req = requests.get(
            url=f"{self.base_url}/",
            headers=self.req_headers,
            params=params,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req.json()

    def get_all(self):
        items = []
        params = self.params
        more_page = True
        while more_page:
            data = self.__get_by_page(params)
            for row in data['results']:
                properties = row['properties']
                items.append(properties)
                
            if 'paging' in data:
                params['after'] = data['paging']['next']['after']
            else:
                more_page = False

        df = pd.DataFrame(items).reset_index(drop=True)
        return df

    def get(self, uid):
        if not uid:
            print(f"Uid can not be None.")
        else:
            try:
                req = requests.get(
                    url=f"{self.base_url}/{uid}",
                    headers=self.req_headers,
                    params=self.params,
                    allow_redirects=False,
                )
                req.raise_for_status()
                # Message success
                print(f"id '{uid}' successfully recovered.")
                return req.json()
            except requests.HTTPError as err:
                err_code = err.response.status_code
                if err_code == 404:
                    print(f"id '{uid}' does not exist. Please, fill the function with an existing id.")
                else:
                    print(err_code)

    def update(self, uid, data):
        if len(uid) == 0:
            print(f"uid is empty")
        else:
            try:
                data = self.__values_format(data)
                req = requests.patch(
                    url=f"{self.base_url}/{uid}",
                    headers=self.req_headers,
                    params=self.params,
                    json=data,
                    allow_redirects=False,
                )
                req.raise_for_status()
                # Message success
                print(f"id '{uid}' successfully updated ({email})")
                return req.json()
            except requests.HTTPError as err:
                err_code = err.response.status_code
                if err_code == 404:
                    print(f"id '{uid}' does not exist. Please, fill the function with an existing id.")
                if err_code == 409:
                     print(f"'{email}' with id '{uid}' already exists.")
                elif err_code == 400:
                     print(f"'Bad Request : syntax error on '{email}'. Please, check the syntax.")
                else:
                    print(err_code)
        
            
    def send(self, data):
        if len(data) == 0:
            print(f"Data is empty")
        try:
            req = requests.post(
                url=f"{self.base_url}/",
                headers=self.req_headers,
                params=self.params,
                json=data,
                allow_redirects=False,
            )
            req.raise_for_status()
            data = req.json()
            data_id = data['id']
            # Message success
            print(f"id '{data_id}' successfully created in Hubspot.")
            return data_id
        except requests.HTTPError as err:
            err_code = err.response.status_code
            if err_code == 404:
                print(f"id '{uid}' does not exist. Please, fill the function with an existing id.")
            if err_code == 409:
                print(f"'{email}' with id '{uid}' already exists.")
            elif err_code == 400:
                print(f"'Bad Request : syntax error on '{email}'. Please, check the syntax.")
            else:
                print(err_code)


    def delete(self, uid):
        if uid == None:
            print(f"uid is empty")
        else:
            req = requests.delete(
                url=f"{self.base_url}{uid}",
                headers=self.req_headers,
                params=self.params,
                allow_redirects=False,
            )
            req.raise_for_status()
            # Message success
            print(f"id {uid} deleted in Hubspot.")

class Contact(HSCRUD):
    def update_contact(self, uid, email=None, firstname=None, lastname=None, company=None, phone=None, jobtitle=None, hubspot_owner_id=None, website=None):     
        data = {"properties": 
                  {
                    "email": email,
                    "firstname": firstname,
                    "lastname": lastname,
                    "website": website,
                    "company": company,
                    "phone": phone,
                    "jobtitle": jobtitle,
                    "hubspot_owner_id": hubspot_owner_id,
                   }
                 }
        result = self.update(uid, data)
        return result
        
    def create(self, email, firstname, lastname, company=None, phone=None, jobtitle=None, hubspot_owner_id=None, website=None):
        data = {"properties": 
                  {
                    "email": email,
                    "firstname": firstname,
                    "lastname": lastname,
                    "website": website,
                    "company": company,
                    "phone": phone,
                    "hubspot_owner_id": hubspot_owner_id,
                   }
                 }
        result = self.send(data)
        return result
    
class Deal(HSCRUD):
    def update_deal(self, uid, dealname=None, dealstage=None, closedate=None, hubspot_owner_id=None, pipeline=None, amount=None):
        data = {"properties": 
                  {
                    "dealstage": dealstage,
                    "dealname": dealname,
                    "pipeline": pipeline,
                    "amount": amount,
                    "closedate": closedate,
                    "hubspot_owner_id": hubspot_owner_id,
                   }
                 }
        result = self.update(uid, data)
        return result
    
    def create(self, dealname, dealstage, closedate=None, hubspot_owner_id=None, pipeline=None, amount=None):
        check_closedate = True
        # Check if close date is in correct format
        if not closedate is None:
            if len(closedate) == 0:
                print(f"Close date '{closedate}' is empty. Please fill in the function.")
                check_closedate = False
            else:
                try:
                    closedate = datetime.datetime.strptime(closedate, "%d/%m/%Y")
                    closedate = str(int(closedate.timestamp()))+"000"
                except:
                    check_closedate = False
                    print(f"Close date '{closedate}' is not in the correct format.\n"
                           "Please change it to %d/%m/%Y.")
        if check_closedate:
            data = {"properties": 
                      {
                        "dealstage": dealstage,
                        "dealname": dealname,
                        "pipeline": pipeline,
                        "amount": amount,
                        "closedate": closedate,
                        "hubspot_owner_id": hubspot_owner_id,
                       }
                     }
            result = self.send(data)
            return result
    
class Pipeline:
    def __init__(self, base_url, auth, req_headers, params):
        self.req_headers = req_headers
        self.params = params
        self.base_url = base_url
        self.model_name = self.base_url.split("/")[-1]

    def get_all_pipeline(self):
        req = requests.get(
            url=f"{self.base_url}/",
            headers=self.req_headers,
            params=self.params,
            allow_redirects=False,
        )
        req.raise_for_status()
        return req.json()

class Pipeline_deal(Pipeline):
    def get_all(self):
        data = self.get_all_pipeline()
        df = pd.DataFrame.from_records(data['results'])
        df = df.drop(['stages'], axis=1)
        df = df.sort_values(by=['displayOrder']).reset_index(drop=True)
        return df
    
class Dealstage(Pipeline):
    def get_all(self):
        data = self.get_all_pipeline()
        items = []
        for row in data['results']:
            pipeline = row['label']
            stages = row['stages']
            id_pipeline = row['id']
            for stage in stages:
                label = stage['label']
                displayOrder = stage['displayOrder']
                id_dealstage = stage['id']
                createdAt = stage['createdAt']
                updatedAt = stage['updatedAt']
                archived = stage['archived']

                deal_stage = {'pipeline': pipeline,
                              'id_pipeline': id_pipeline,
                              'dealstage': label,
                              'id_dealstage': id_dealstage,
                              'displayOrder': displayOrder,
                              'createdAt': createdAt,
                              'updatedAt': updatedAt,
                              'archived': archived}
            
                items.append(deal_stage)
      
        df = pd.DataFrame(items)
        df = df.sort_values(by=['pipeline', 'displayOrder'])
        df = df.reset_index(drop=True)
        return df
    
class Association:
    def __init__(self, base_url, auth, req_headers, params):
        self.req_headers = req_headers
        self.params = params
        self.base_url = base_url
        self.model_name = self.base_url.split("/")[-1]

    def get(self, objet, objet_id, associate):
        try:
            req = requests.get(
                url=f"{self.base_url}{objet}/{objet_id}/associations/{associate}",
                headers=self.req_headers,
                params=self.params,
                allow_redirects=False,
            )
            req.raise_for_status()
            data = req.json()
            df = pd.DataFrame.from_records(data['results'])
            return df
        except requests.HTTPError as err:
            err_code = err.response.status_code
            if err_code == 400:
                raise ValueError(f"Bad Request : syntax error on '{objet}' or '{associate}'. Please check the possibilities : 'contact', 'deal', 'company'")
            else:
                print(err.response.json())
    
    def send(self, objet, objet_id, associate, id_associate, associationType):
        try:
            req = requests.put(
                url=f"{self.base_url}{objet}/{objet_id}/associations/{associate}/{id_associate}/{associationType}",
                headers=self.req_headers,
                params=self.params,
                allow_redirects=False,
            )
            req.raise_for_status()
            print(f"{objet} '{objet_id}' and {associate} '{id_associate}' successfully associated !")
        except requests.HTTPError as err:
            err_code = err.response.status_code
            if err_code == 400:
                print(f"Bad Request : syntax error on '{objet}' or '{associate}'. Please check the possibilities : 'contact', 'deal', 'company'")
            else:
                print(err.response.json())

class Hubspot(InDriver, OutDriver):

    base_url = os.environ.get(
        "HUBSPOT_API_URL", "https://api.hubapi.com/crm/v3"
    )
    api_token = None

    def connect(self, api_token):
        # Init Hubspot attribute
        self.token = api_token
        self.req_headers = {'accept': "application/json",
                            'content-type': "application/json"}
        self.params = {"limit": "100",
                       "archived": "false",
                       "hapikey": api_token}
        
        # Init end point
        self.contacts = Contact(f"{self.base_url}/objects/contacts/", self.token, self.req_headers, self.params)
        self.company = HSCRUD(f"{self.base_url}/objects/company/", self.token, self.req_headers, self.params)
        self.deals = Deal(f"{self.base_url}/objects/deals/", self.token, self.req_headers, self.params)
        self.pipelines = Pipeline_deal(f"{self.base_url}/pipelines/deals", self.token, self.req_headers, self.params)
        self.dealstages = Dealstage(f"{self.base_url}/pipelines/deals", self.token, self.req_headers, self.params)
        self.associations = Association(f"{self.base_url}/objects/", self.token, self.req_headers, self.params)
        
        # Set connexion to active
        self.connected = True
        return self
