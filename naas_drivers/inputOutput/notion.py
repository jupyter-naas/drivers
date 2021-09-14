import requests
from typing import Dict
import json
import pandas as pd
from abc import ABC, abstractmethod

VERSION = "2021-08-16"

"""
CONNECT
"""
class Notion:
    headers = {}

    @classmethod
    def connect(cls, token: str) -> None:
        cls.headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": f"{VERSION}",
            "Content-Type": "application/json",
        }


"""
REQUEST
"""

def catch_error(response):
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        if response.status_code == 401:
            raise Exception(
                "❌ Connect to Notion with your Token: Notion.connect(YOUR_TOKEN_API)"
            )
        else:
            raise Exception("❌", err, response.text)


class RequestNotionAPI:
    def __init__(self, headers: Dict, id: str = None) -> None:
        self.headers = headers
        self.id = id


class RequestDatabase(RequestNotionAPI):
    URL = "https://api.notion.com/v1/databases"

    def create(self, data):
        pass


class RequestPage(RequestNotionAPI):
    URL = "https://api.notion.com/v1/pages/"

    def retreive(self) -> Dict:
        url = self.URL + self.id
        response = requests.get(url, headers=self.headers)
        catch_error(response)
        return response.json()

    def create(self, data) -> Dict:
        data = json.dumps(data)
        response = requests.post(self.URL, headers=self.headers, data=data)
        catch_error(response)
        print("✅ Page has been created")
        return response.json()

    def update(self, data):
        url = self.URL + self.id
        data = json.dumps(data)
        response = requests.patch(url, headers=self.headers, data=data)
        catch_error(response)
        print("✨ Properties have been updated")


class RequestBlock(RequestNotionAPI):
    URL = "https://api.notion.com/v1/blocks/"

    def update(self, data):
        url = self.URL + self.id
        data = json.dumps(data)
        response = requests.patch(url, headers=self.headers, data=data)
        catch_error(response)
        print("✅ Block has been updated")

    def retreive_children(self) -> Dict:
        url = self.URL + self.id + "/children"
        response = requests.get(url, headers=self.headers)
        catch_error(response)
        return response.json()["results"]

    def append_children(self, data):
        url = self.URL + self.id + "/children"

        data = {"children": [data]}
        data = json.dumps(data)

        response = requests.patch(url, headers=self.headers, data=data)
        catch_error(response)
        print("✅ Block have been add to your page")

    def delete(self):
        url = self.URL + self.id
        response = requests.delete(url, headers=self.headers)
        catch_error(response)
        print("🌪 Block has been deleted")
        
"""
Property
"""
        
class Property:
    """An object describing by an id, type, and value of a page property."""

    def __init__(self, property_object) -> None:
        self.raw = property_object
        self.id = property_object["id"]
        self.type = property_object["type"]
        self.value = property_object[self.type]

    def extract(self):
        if self.value is None:
            return None
        else:
            if self.type == "date":
                if self.value["end"]:
                    return f'{self.value["start"]} -> {self.value["end"]}'
                return self.value["start"]

            elif self.type in ["title", "rich_text"]:
                texts = [text["plain_text"] for text in self.value]
                return ",".join(texts)

            elif self.type == "select":
                return self.value["name"]

            elif self.type == "multi_select":
                selections = [select["name"] for select in self.value]
                return ", ".join(selections)

            elif self.type in ["number", "url", "phone_number", "email", "checkbox"]:
                return self.value

            elif self.type == "people":
                peoples = [
                    people.get("name") for people in self.value if people.get("name")
                ]
                return ", ".join(peoples)

    def insert(self, value):
        if self.type == "date":
            if isinstance(value, list) and len(value) == 2:
                self.value["start"] = value[0]
                self.value["end"] = value[1]
            elif isinstance(value, str):
                self.value["start"] = value
                self.value["end"] = None
            else:
                raise TypeError(
                    "Date must be a '2021-08-28' or ['2021-08-28', '2021-10-28']"
                )

        if self.type in ["title", "rich_text"]:
            if isinstance(value, str):
                del self.value[1:]
                self.value[0]["plain_text"] = value
                self.value[0]["text"]["content"] = value
            else:
                raise TypeError(f"{self.type} must be a string")

        if self.type == "select":
            if isinstance(value, str):
                self.raw[self.type] = {"name": value}
            else:
                raise TypeError(f"{self.type} must be a string")

        if self.type == "multi_select":
            if isinstance(value, str):
                self.raw[self.type].clear()
                self.raw[self.type] = [{"name": value}]
            elif isinstance(value, list):
                self.raw[self.type].clear()
                for elm in value:
                    self.raw[self.type].append({"name": elm})
            else:
                raise TypeError(f"{self.type} must be a string or a list of string")

        elif self.type == "number":
            if isinstance(value, int):
                self.raw[self.type] = value
            else:
                raise TypeError(f"{self.type} must be an integer")

        elif self.type in ["url", "phone_number", "email"]:
            if isinstance(value, str):
                self.raw[self.type] = value
            else:
                raise TypeError(f"{self.type} must be a string or a list of string")

            
"""
BLOCK
"""
 
class Block(ABC):
    type = None

    def __init__(self, dictionary: Dict) -> None:
        self.object = dictionary.get("object")
        self.id = dictionary.get("id")
        self.type = dictionary.get("type")
        self.created_time = dictionary.get("created_time")
        self.last_edited_time = dictionary.get("last_edited_time")
        self.has_children = dictionary.get("has_children")
        self.value = dictionary.get(self.type)

    def __repr__(self) -> str:
        return self.get()

    @abstractmethod
    def get(self):
        "Implemented in the subclass"

    @abstractmethod
    def set(self, value):
        "Implemented in the subclass"

    @abstractmethod
    def create(self, value):
        "Implemented in the subclass"

    def update(self):
        data = {self.type: self.value}
        RequestBlock(Notion.headers, self.id).update(data)

    def delete(self):
        RequestBlock(Notion.headers, self.id).delete()


class TextBLock(Block):
    type = None

    def get(self):
        content = [rich_text["plain_text"] for rich_text in self.value["text"]]
        return " ".join(content)

    def set(self, value: str):
        del self.value["text"][1:]
        self.value["text"][0]["text"]["content"] = value
        self.value["text"][0]["plain_text"] = value

    @classmethod
    def create(cls, text: str, link: str = None):
        return {
            "type": cls.type,
            cls.type: {
                "text": [{"type": "text", "text": {"content": text, "link": link}}]
            },
        }


class Paragraph(TextBLock):
    type = "paragraph"


class Heading1(TextBLock):
    type = "heading_1"


class Heading2(TextBLock):
    type = "heading_2"


class Heading3(TextBLock):
    type = "heading_3"


class BulletedList(TextBLock):
    type = "bulleted_list_item"


class NumberedList(TextBLock):
    type = "numbered_list_item"


class ToDo(TextBLock):
    type = "to_do"

    @classmethod
    def create(cls, text: str, checked: bool = False, link: str = None):
        return {
            "type": cls.type,
            cls.type: {
                "text": [
                    {
                        "type": "text",
                        "text": {"content": text, "link": link},
                    }
                ],
                "checked": checked,
            },
        }


class Toggle(TextBLock):
    type = "toggle"


class ChildPage(Block):
    type = "child_page"

    def get(self):
        return self.value["title"]

    def set(self, value):
        self.value["title"] = value

    @classmethod
    def create(cls, title: str):
        return {
            "type": cls.type,
            "properties": {"title": [{"text": {"content": title}}]},
        }


class Embed(Block):
    type = "embed"

    def get(self):
        return self.value["url"]

    def set(self, value):
        self.value["url"] = value

    @staticmethod
    def create(url: str):
        return {"type": "embed", "embed": {"url": url}}


mapping = {
    "paragraph": Paragraph,
    "heading_1": Heading1,
    "heading_2": Heading2,
    "heading_3": Heading3,
    "bulleted_list_item": BulletedList,
    "numbered_list_item": NumberedList,
    "to_do": ToDo,
    "toggle": Toggle,
    "child_page": ChildPage,
    "embed": Embed,
}


def extract_block(block_object):
    block_type = block_object.get("type")
    return mapping[block_type](block_object)


def insert_block(block_object, value):
    block_type = block_object.get("type")
    return mapping[block_type](block_object).set(value)


"""
PAGE
"""

class PageProperties:
    def __init__(self, properties: Dict, headers: Dict):
        self.headers = headers

        self.raw = properties
        self._properties = properties["properties"]
        self.parent_id = properties["id"]

    def __getitem__(self, key):
        return Property(self._properties[key]).extract()

    def __setitem__(self, key, value):
        Property(self._properties[key]).insert(value)

    def __repr__(self) -> str:
        return f"{self.get()}"

    def get(self) -> pd.Series:
        data = {key: self[key] for key in self._properties.keys()}
        return pd.Series(data)

    def update(self) -> None:
        RequestPage(self.headers, self.parent_id).update(self.raw)


class PageContent:
    def __init__(self, blocks, page_id, headers) -> None:
        self.raw = blocks
        self.page_id = page_id
        self.headers = headers

    def __getitem__(self, index):
        return extract_block(self.raw[index])

    def __setitem__(self, index, value):
        insert_block(self.raw[index], value)

    def __repr__(self) -> str:
        return f"{self.get()}"

    def _repr_html_(self):
        return self.get().to_html()

    def get(self) -> pd.DataFrame:
        result = []
        for block in self.raw:
            result.append(
                {
                    "type": block.get("type"),
                    "content": extract_block(block),
                    "id": block.get("id"),
                }
            )
        return pd.DataFrame(result)

    def append(self, block: Dict) -> None:
        if block["type"] == "child_page":
            block.pop("type")
            block["parent"] = {"page_id": self.page_id}
            # import pdb

            # pdb.set_trace()
            RequestPage(self.headers).create(block)
        else:
            RequestBlock(self.headers, self.page_id).append_children(block)

"""
LOW CODE OBJECT
"""

class Database:
    def __init__(self, database_url) -> None:
        self.url = database_url

    @property
    def id(self):
        path = self.url.split("/")[-1]
        return path.split("?")[0]

    def create_new_page(self):
        data = {
            "parent": {"database_id": self.id},
            "properties": {"title": [{"text": {"content": "Undifined"}}]},
        }
        raw_page = RequestPage(Notion.headers).create(data)
        return Page(raw_page["url"])


class Page:
    def __init__(self, page_url) -> None:
        self.url = page_url
        self.id = page_url.split("-")[-1]

        self._properties = RequestPage(Notion.headers, self.id).retreive()
        self.created_time = self._properties["created_time"]
        self.last_edited_time = self._properties["last_edited_time"]
        self.archived = self._properties["archived"]
        self.icon = self._properties["icon"]
        self.cover = self._properties["cover"]
        self.properties = PageProperties(self._properties, Notion.headers)
        self.parent = self._properties["parent"]

        self._content = RequestBlock(Notion.headers, self.id).retreive_children()
        self.content = PageContent(self._content, self.id, Notion.headers)

    def refresh(self):
        """Retreive Page properties & content."""
        return self.__init__(self.url)

    def delete(self):
        """Archiving workspace level pages via API not supported."""
        RequestBlock(Notion.headers, self.id).delete()