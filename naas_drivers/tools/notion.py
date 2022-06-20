from naas_drivers.driver import InDriver, OutDriver
import dataclasses
from dataclasses import dataclass, field
from typing import List, Optional, Union
from numbers import Number
from enum import Enum
import json
import logging
from copy import deepcopy
import pandas as pd
import threading
import pydash
from notion_client import Client
from dacite import from_dict


VERSION = "2021-08-16"

global notion_instance


def ensure_page_id(page_id):
    if "http" in page_id:
        page_id = page_id.split("/")[-1].split("-")[-1]
    return page_id


def ensure_database_id(database_id):
    if "http" in database_id:
        database_id = database_id.split("/")[-1].split("?")[0]
    return database_id


class Notion(InDriver, OutDriver):
    __client = None

    def __init__(self):
        """
        Classes / Methods to query notion
        """
        self.database = self.Databases(self)
        self.databases = self.database

        self.page = self.Pages(self)
        self.pages = self.page

        self.block = self.Blocks(self)
        self.blocks = self.block

        self.user = self.Users(self)
        self.users = self.user

        """
        Data classes representing Page/Database/Blocks.
        """
        self.Page = Page
        self.Database = Database
        self.Block = Block

        """
        Global to allow all dataclasses to access notion client and make requests
        from a dataclass directly.
        """
        global notion_instance
        notion_instance = self

    @staticmethod
    def instance():
        return notion_instance

    @property
    def client(self):
        return self.__client

    def connect(self, token: str, log_level: int = logging.WARNING):
        self.__client = Client(auth=token, log_level=log_level)
        return self

    def to_json(self, source):
        return json.dumps(source, cls=EnhancedJSONEncoder, ensure_ascii=False)

    def to_dict(self, source):
        return del_none(json.loads(self.to_json(source)))

    class __InnerBase:
        __parent = None

        def __init__(self, parent):
            self.__parent = parent

        @property
        def client(self):
            return self.__parent.client

        @property
        def parent(self):
            return self.__parent

    class Databases(__InnerBase):
        def __from_dict(self, data):
            return from_dict(data_class=Database, data=data)

        def query(self, database_id, query={}):
            database_id = ensure_database_id(database_id)
            ret = []
            has_more = True
            next_cursor = None

            while has_more:
                if has_more is True and next_cursor is not None:
                    query["start_cursor"] = next_cursor

                response = self.client.databases.query(database_id=database_id, **query)
                results = response.get("results")

                for r in results:
                    ret.append(from_dict(data_class=Page, data=r))

                if response.get("has_more") is True:
                    has_more = True
                    next_cursor = response.get("next_cursor")
                else:
                    has_more = False

            return ret

        def create(self, db):
            payload = self.parent.to_dict(db)
            return self.__from_dict(self.client.databases.create(**payload))

        def update(self, db):
            payload = self.parent.to_dict(db)
            return self.__from_dict(
                self.client.databases.update(database_id=db.id, **payload)
            )

        def get(self, database_id):
            return self.retrieve(database_id)

        def retrieve(self, database_id):
            database_id = ensure_database_id(database_id)
            raw = self.client.databases.retrieve(database_id)
            return self.__from_dict(raw)

    class Pages(__InnerBase):
        def __from_dict(self, data):
            return from_dict(data_class=Page, data=data)

        def create_from_page(self, page):
            payload = self.parent.to_dict(page)
            return self.__from_dict(self.client.pages.create(**payload))

        def create(self, database_id, title):
            database_id = ensure_database_id(database_id)
            new_page = Page.new(database_id=database_id)
            new_page.title("Name", title)
            return new_page.create()

        def retrieve(self, page_id: str):
            page_id = ensure_page_id(page_id)
            page = self.client.pages.retrieve(page_id=page_id)
            return self.__from_dict(page)

        def get(self, page_id: str):
            return self.retrieve(page_id)

        def update(self, page):  # TODO: Improve readability.
            copied = deepcopy(page)
            filtered_properties = {}
            for p in copied.properties:
                prop = copied.properties[p]
                if (
                    prop.type
                    not in [
                        "last_edited_by",
                        "last_edited_time",
                        "created_by",
                        "created_time",
                    ]
                    and getattr(prop, prop.type) is not None
                    and not (
                        prop.type == "rich_text"
                        and pydash.get(getattr(prop, prop.type), "[0].plain_text") == ""
                    )
                ):
                    filtered_properties[p] = prop
            copied.properties = filtered_properties
            payload = self.parent.to_dict(copied)
            return self.__from_dict(
                self.client.pages.update(page_id=copied.id, **payload)
            )

    class Blocks(__InnerBase):
        def __from_dict(self, data):
            return from_dict(data_class=Block, data=data)

        def retrieve(self, block_id: str):
            block = self.client.blocks.retrieve(block_id=block_id)
            return self.__from_dict(block)

        def get(self, block_id: str):
            return self.retrieve(block_id)

        def children(self, block_id: str):
            ret = []
            blocks = self.client.blocks.children.list(block_id=block_id).get("results")
            for r in blocks:
                ret.append(self.__from_dict(r))
            return ret

        def append(self, block_id, blocks):
            if type(blocks) != list:
                blocks = [blocks]
            payload = self.parent.to_dict(blocks)
            ret = []
            for b in self.client.blocks.children.append(
                block_id=block_id, children=payload
            ).get("results"):
                ret.append(self.__from_dict(b))
            return ret

        def update(self, block):
            payload = self.parent.to_dict(block)
            return self.__from_dict(
                self.client.blocks.update(block_id=block.id, **payload)
            )

        def delete(self, block_id):
            return self.__from_dict(self.client.blocks.delete(block_id=f"{block_id}"))

    class Users(__InnerBase):
        def __from_dict(self, data):
            return from_dict(data_class=User, data=data)

        def retrieve(self, user_id):
            return self.__from_dict(self.client.users.retrieve(user_id=user_id))

        def get(self, user_id):
            return self.retrieve(user_id)

        def list(self):
            ret = []
            for b in self.client.users.list().get("results"):
                ret.append(self.__from_dict(b))
            return ret


"""Notion Data classes"""


class __BaseDataClass:
    """
    __BaseDataClass is used to make it easy from any @dataclass to access notion client.
    It allows to update/create/delete/query objects directly from a dataclass.
    """

    def to_json(self, source=None):
        if source is None:
            source = self
        return json.dumps(source, cls=EnhancedJSONEncoder)

    @property
    def notion(self):
        return Notion.instance()


@dataclass
class FileExternal:
    url: Optional[str] = ""

    @classmethod
    def new(cls, url: str) -> "FileExternal":
        return cls(url=url)


@dataclass
class File(__BaseDataClass):
    url: Optional[str]
    expiry_time: Optional[str]
    name: Optional[str]
    external: Optional[FileExternal] = field(default_factory=FileExternal)
    type: str = "external"

    @classmethod
    def new(cls, url: str) -> "File":
        return cls(
            url=None, expiry_time=None, name=None, external=FileExternal(url=url)
        )


@dataclass
class Emoji(__BaseDataClass):
    emoji: str
    type: str = "emoji"

    @classmethod
    def new(cls, emoji: str) -> "Emoji":
        return cls(emoji=emoji)


@dataclass
class Parent(__BaseDataClass):
    type: str
    workspace: Optional[bool] = None
    page_id: Optional[str] = None
    database_id: Optional[str] = None

    @classmethod
    def new(cls, type: str, value: Union[bool, str]) -> "Parent":
        if type == "page_id":
            return cls(type=type, page_id=value)
        elif type == "database_id":
            return cls(type=type, database_id=value)
        elif type == "workspace":
            return cls(type=type, workspace=value)
        else:
            return cls(type=type)

    @classmethod
    def new_page_parent(cls, page_id):
        return cls(type="page_id", page_id=page_id)

    @classmethod
    def new_workspace_parent(cls):
        return cls(type="workspace", workspace=True)

    @classmethod
    def new_database_parent(cls, database_id):
        return cls(type="database_id", database_id=database_id)


@dataclass
class Link(__BaseDataClass):
    url: str
    type: str = "url"

    @classmethod
    def new(cls, url: str) -> "Link":
        return cls(url=url)


@dataclass
class Text(__BaseDataClass):
    content: str
    link: Optional[Link] = None

    @classmethod
    def new(cls, content: str) -> "Text":
        return cls(content=content)


@dataclass
class Annotation(__BaseDataClass):
    bold: bool = False
    italic: bool = False
    strikethrough: bool = False
    underline: bool = False
    code: bool = False
    color: str = "default"

    @classmethod
    def new(cls) -> "Annotation":
        return cls()


@dataclass
class Person(__BaseDataClass):
    email: str

    @classmethod
    def new(cls, email: str) -> "Person":
        return cls(email=email)


@dataclass
class User(__BaseDataClass):
    object: str
    id: str
    type: Optional[str]
    name: Optional[str]
    avatar_url: Optional[str]
    person: Optional[Person]
    bot: Optional[object]

    @classmethod
    def new(cls, user_id: str) -> "User":
        return notion_instance.users.get(user_id)

    def __post_init__(self):
        if self.type == "person":
            self.person = Person("")
        if self.type == "bot":
            self.bot = {}


@dataclass
class Date(__BaseDataClass):
    start: str
    end: Optional[str]

    @classmethod
    def new(cls, start: str) -> "Date":
        return cls(start=start)


@dataclass
class Equation(__BaseDataClass):
    expression: str

    @classmethod
    def new(cls, expression: str) -> "Equation":
        return from_dict(data_class=cls, expression=expression)


@dataclass
class UserMention(__BaseDataClass):
    type: str
    user: User


@dataclass
class RichText(__BaseDataClass):
    plain_text: str
    href: Optional[str] = None
    annotations: Annotation = field(default_factory=Annotation)
    type: str = "text"
    text: Optional[Text] = None
    mention: Optional[UserMention] = None
    page: Optional[object] = None
    database: Optional[object] = None
    date: Optional[Date] = None
    equation: Optional[Equation] = None

    @classmethod
    def new(cls, text: str) -> "RichText":
        # Notion does not accept empty text anymore, we added spaces to fix for now (2022-02-23)
        if text == "":
            return cls(type="text", plain_text=" ", text=Text.new(" "))
        return cls(type="text", plain_text=text, text=Text.new(text))

    @classmethod
    def new_text(cls, content):
        # Notion does not accept empty text anymore, we added spaces to fix for now (2022-02-23)
        if content == "":
            return cls(plain_text=" ", text=Text(content=" "))
        return cls(plain_text=content, text=Text(content=content))


"""
Database and Database properties

This section is containing all classes related to Properties (https://developers.notion.com/reference/database#property-object)
"""


@dataclass
class Property(__BaseDataClass):
    id: Optional[str] = None
    type: str = None  # TODO: Consider using enum to improve typing.


@dataclass
class DatabaseProperty(Property):
    name: str = None


@dataclass
class DatabasePropertyTitle(DatabaseProperty):
    title: object = field(default_factory=dict)
    type: str = "title"


@dataclass
class DatabasePropertyText(DatabaseProperty):
    rich_text: object = field(default_factory=dict)
    type: str = "rich_text"


@dataclass
class DatabasePropertyNumber_Configuration:
    format: str = "number"


@dataclass
class DatabasePropertyNumber(DatabaseProperty):
    number: DatabasePropertyNumber_Configuration = field(default_factory=dict)
    type: str = "number"


@dataclass
class DatabasePropertySelect_Configuration_Option:
    name: str
    id: str
    color: str  # TODO: enum


@dataclass
class DatabasePropertySelect_Configuration:
    options: List[DatabasePropertySelect_Configuration_Option] = field(
        default_factory=list
    )


@dataclass
class DatabasePropertySelect(DatabaseProperty):
    select: DatabasePropertySelect_Configuration = field(
        default_factory=DatabasePropertySelect_Configuration
    )
    type: str = "select"

    def get_option_by_name(self, name):
        if self.select and self.select.options:
            for option in self.select.options:
                if option.name == name:
                    return option
        return None


@dataclass
class DatabasePropertyMultiSelect(DatabaseProperty):
    multi_select: DatabasePropertySelect_Configuration = field(
        default_factory=DatabasePropertySelect_Configuration
    )  # TODO: Create own childs? Not reuse DatabasePropertySelect_Configuration class.
    type: str = "multi_select"

    def get_option_by_name(self, name):
        if self.multi_select and self.multi_select.options:
            for option in self.multi_select.options:
                if option.name == name:
                    return option
        return None


@dataclass
class DatabasePropertyDate(DatabaseProperty):
    date: object = field(default_factory=dict)
    type: str = "date"


@dataclass
class DatabasePropertyPeople(DatabaseProperty):
    people: object = field(default_factory=dict)
    type: str = "people"


@dataclass
class DatabasePropertyFiles(DatabaseProperty):
    files: object = field(default_factory=dict)
    type: str = "files"


@dataclass
class DatabasePropertyCheckbox(DatabaseProperty):
    checkbox: object = field(default_factory=dict)
    type: str = "checkbox"


@dataclass
class DatabasePropertyUrl(DatabaseProperty):
    url: object = field(default_factory=dict)
    type: str = "url"


@dataclass
class DatabasePropertyEmail(DatabaseProperty):
    email: object = field(default_factory=dict)
    type: str = "email"


@dataclass
class DatabasePropertyPhoneNumber(DatabaseProperty):
    phone_number: object = field(default_factory=dict)
    type: str = "phone_number"


@dataclass
class DatabasePropertyFormula_Configuration:
    expression: str = ""
    type: str = "expression"


@dataclass
class DatabasePropertyFormula(DatabaseProperty):
    formula: DatabasePropertyFormula_Configuration = field(
        default_factory=DatabasePropertyFormula_Configuration
    )
    type: str = "formula"


@dataclass
class DatabasePropertyRelation_Configuration:
    database_id: str
    synced_property_name: Optional[str]
    synced_property_id: Optional[str]


@dataclass
class DatabasePropertyRelation(DatabaseProperty):
    relation: DatabasePropertyRelation_Configuration = field(
        default_factory=DatabasePropertyRelation_Configuration
    )
    type: str = "relation"


@dataclass
class DatabasePropertyRollup_Configuration:
    relation_property_name: str
    relation_property_id: str
    rollup_property_name: str
    rollup_property_id: str
    function: str


@dataclass
class DatabasePropertyRollup(DatabaseProperty):
    rollup: DatabasePropertyRollup_Configuration = field(
        default_factory=DatabasePropertyRollup_Configuration
    )
    type: str = "rollup"


@dataclass
class DatabasePropertyCreatedBy(DatabaseProperty):
    created_by: object = field(default_factory=dict)
    type: str = "created_by"


@dataclass
class DatabasePropertyCreatedTime(DatabaseProperty):
    created_time: object = field(default_factory=dict)
    type: str = "created_time"


@dataclass
class DatabasePropertyLastEditedTime(DatabaseProperty):
    last_edited_time: object = field(default_factory=dict)
    type: str = "last_edited_time"


@dataclass
class DatabasePropertyLastEditedBy(DatabaseProperty):
    last_edited_by: object = field(default_factory=dict)
    type: str = "last_edited_by"


class DatabasePropertyFactory:
    """
    This class is a helper to create the proper DatabaseProperty type automaticaly.
    """

    __rel_map = {
        "title": DatabasePropertyTitle,
        "rich_text": DatabasePropertyText,
        "number": DatabasePropertyNumber,
        "select": DatabasePropertySelect,
        "multi_select": DatabasePropertyMultiSelect,
        "date": DatabasePropertyDate,
        "people": DatabasePropertyPeople,
        "files": DatabasePropertyFiles,
        "checkbox": DatabasePropertyCheckbox,
        "url": DatabasePropertyUrl,
        "email": DatabasePropertyEmail,
        "phone_number": DatabasePropertyPhoneNumber,
        "formula": DatabasePropertyFormula,
        "relation": DatabasePropertyRelation,
        "rollup": DatabasePropertyRollup,
        "created_by": DatabasePropertyCreatedBy,
        "created_time": DatabasePropertyCreatedTime,
        "last_edited_time": DatabasePropertyLastEditedTime,
        "last_edited_by": DatabasePropertyLastEditedBy,
    }

    @staticmethod
    def new(data):
        data_type = data.get("type")
        if data_type and data_type in DatabasePropertyFactory.__rel_map:
            return from_dict(
                data_class=DatabasePropertyFactory.__rel_map[data_type], data=data
            )
        else:
            raise Exception(f'DatabaseProperty "{data_type}" not implemented yet.')


@dataclass
class Database(__BaseDataClass):
    title: List[RichText]
    properties: object
    parent: Parent
    # icon: Optional[Emoji] = None  # TODO: Handle File
    cover: Optional[File] = None
    created_time: str = None
    last_edited_time: str = None
    url: str = None
    object: str = "database"
    id: str = None

    """
    __post_init__ is called right after the object is created.

    We use it here to keep self.properties as a Dictionnary but we
    want to convert values as Data Classes instances.
    """

    def __post_init__(self):
        for k in self.properties:
            self.properties[k] = DatabasePropertyFactory.new(self.properties[k])

    def __repr__(self):
        try:
            display(self.df())  # noqa: F821
            return ""
        except:  # noqa: E722
            return str(self)

    def schema(self):
        copied = deepcopy(self.properties)
        d = []
        for i in copied:
            p = copied[i]
            d.append({"Name": i, "Type": p.type})
        return pd.DataFrame(self.notion.to_dict(d))

    @classmethod
    def new(cls, title: str, page_id: str = None):
        parent = None
        if page_id:
            parent = Parent.new_page_parent(page_id=page_id)
        else:
            parent = Parent.new_workspace_parent()

        new_db = cls(title=[RichText.new(title)], parent=parent, properties={})
        new_db.add_property("Name", "title")
        return new_db

    @classmethod
    def from_dict(cls, data):
        return from_dict(data_class=cls, data=data)

    # def set_emoji_icon(
    #    self, data
    # ):  # TODO: Fix, seems like there is an issue with notion-client / httpx maybe.
    #    self.icon = Emoji(data)

    def duplicate(self):
        new_db = self.create()
        pages = self.query()
        for page in pages:
            page.parent.database_id = new_db.id
            page.duplicate()

        return new_db

    def add_property(self, col_name, type_name):
        data = {"type": type_name, "name": col_name, "id": ""}
        self.properties[col_name] = DatabasePropertyFactory.new(data)

    def df(self):
        pages = self.query({"page_size": 10})
        frames = []

        for page in pages:
            frame = {}
            for prop in page.properties:
                frame[prop] = str(page.properties[prop])
            frames.append(frame)

        return pd.DataFrame(frames)

    def query(self, query={}):
        return self.notion.databases.query(self.id, query)

    def update(self):
        return self.notion.databases.update(self)

    def create(self):
        return self.notion.databases.create(self)


"""
Page and Page properties
"""


@dataclass
class PageProperty(Property):
    pass


@dataclass
class PagePropertyTitle(PageProperty):
    title: Optional[List[RichText]] = field(default_factory=list)
    type: str = "title"

    @classmethod
    def new(cls, content: str) -> "PagePropertyTitle":
        return cls(title=[RichText.new(content)])

    def set_text(self, content: str):
        self.title = [RichText.new_text(content)]

    def __str__(self):
        return " ".join([v.plain_text for v in self.title])


@dataclass
class PagePropertyText(PageProperty):
    rich_text: Optional[List[RichText]] = field(default_factory=list)
    type: str = "rich_text"

    @classmethod
    def new(cls, content: str) -> "PagePropertyText":
        return cls(rich_text=[RichText.new(content)])

    def __str__(self):
        return " ".join([v.plain_text for v in self.rich_text])


@dataclass
class PagePropertyNumber(PageProperty):
    number: Optional[Number] = None
    type: str = "number"

    @classmethod
    def new(cls, number: Number) -> "PagePropertyNumber":
        return cls(number=number)

    def __str__(self):
        return str(self.number)


@dataclass
class PagePropertySelect_Value:
    name: str
    id: str = None
    color: str = "default"

    @classmethod
    def new(cls, name: str, color: str = "default") -> "PagePropertySelect_Value":
        return cls(name=name, color=color)


@dataclass
class PagePropertySelect(PageProperty):
    select: Optional[PagePropertySelect_Value] = field(
        default_factory=PagePropertySelect_Value
    )
    type: str = "select"

    @classmethod
    def new(cls, select: str, color: str = "default") -> "PagePropertySelect":
        return cls(select=PagePropertySelect_Value.new(name=select, color=color))

    def __str__(self):
        return str(self.select.name) if self.select else ""


@dataclass
class PagePropertyMultiSelect(PageProperty):
    multi_select: Optional[List[PagePropertySelect_Value]] = field(
        default_factory=list
    )  # TODO: Create own Value type.
    type: str = "multi_select"

    @classmethod
    def new(
        cls, values: List[str], color: str = "default"
    ) -> "PagePropertyMultiSelect":
        return cls(
            multi_select=[PagePropertySelect_Value.new(v, color) for v in values]
        )

    def __str__(self):
        return ", ".join([v.name for v in self.multi_select])


@dataclass
class PagePropertyDate_Value:
    start: str = None
    end: Optional[str] = None

    @classmethod
    def new(cls, start=str) -> "PagePropertyDate_Value":
        return cls(start=start)


@dataclass
class PagePropertyDate(PageProperty):
    date: Optional[PagePropertyDate_Value] = field(
        default_factory=PagePropertyDate_Value
    )
    type: str = "date"

    @classmethod
    def new(cls, date: str) -> "PagePropertyDate":
        return cls(date=PagePropertyDate_Value.new(date))

    def __str__(self):
        return str(self.date.start) if self.date else ""


@dataclass
class PagePropertyFormula(PageProperty):
    string: Optional[str] = None
    number: Optional[Number] = None
    boolean: Optional[bool] = None
    date: Optional[PagePropertyDate_Value] = field(
        default_factory=PagePropertyDate_Value
    )
    type: str = "formula"

    # TODO: classmethod new

    # TODO: classmethod new


@dataclass
class PagePropertyRelation(PageProperty):
    relation: List[object] = field(default_factory=list)
    type: str = "relation"

    # TODO: classmethod new


@dataclass
class PagePropertyRollup(PageProperty):
    number: Optional[Number] = None
    date: Optional[PagePropertyDate_Value] = field(
        default_factory=PagePropertyDate_Value
    )
    array: Optional[List[object]] = field(default_factory=list)
    type: str = "rollup"

    # TODO: classmethod new


@dataclass
class PagePropertyPeople(PageProperty):
    people: Optional[List[User]] = field(default_factory=list)
    type: str = "people"

    @classmethod
    def new(cls, people: List[str]) -> "PagePropertyPeople":
        return cls(people=[User.new(v) for v in people])

    def __str__(self):
        return " ".join([v.name for v in self.people])


@dataclass
class PagePropertyFiles(PageProperty):
    files: Optional[List[File]] = field(default_factory=list)
    type: str = "files"

    @classmethod
    def new(cls, files: List[str]) -> "PagePropertyFiles":
        return cls(files=[File.new(v) for v in files])

    def __str__(self):
        return ", ".join([v.name for v in self.files])


@dataclass
class PagePropertyCheckbox(PageProperty):
    checkbox: bool = None
    type: str = "checkbox"

    @classmethod
    def new(cls, checkbox: bool) -> "PagePropertyCheckbox":
        return cls(checkbox=checkbox)

    def __str__(self):
        return str(self.checkbox)


@dataclass
class PagePropertyUrl(PageProperty):
    url: Optional[str] = None
    type: str = "url"

    @classmethod
    def new(cls, url: str) -> "PagePropertyUrl":
        return cls(url=url)

    def __str__(self):
        return str(self.url)


@dataclass
class PagePropertyEmail(PageProperty):
    email: Optional[str] = None
    type: str = "email"

    @classmethod
    def new(cls, email: str) -> "PagePropertyEmail":
        return cls(email=email)

    def __str__(self):
        return str(self.email)


@dataclass
class PagePropertyPhoneNumber(PageProperty):
    phone_number: Optional[str] = None
    type: str = "phone_number"

    @classmethod
    def new(cls, phone_number: str) -> "PagePropertyPhoneNumber":
        return cls(phone_number=phone_number)

    def __str__(self):
        return str(self.phone_number)


@dataclass
class PagePropertyCreatedBy(PageProperty):
    created_by: User = field(default_factory=User)
    type: str = "created_by"

    def __str__(self):
        return str(self.created_by.name) if self.created_by else ""


@dataclass
class PagePropertyCreatedTime(PageProperty):
    created_time: str = None
    type: str = "created_time"

    def __str__(self):
        return str(self.created_time)


@dataclass
class PagePropertyLastEditedTime(PageProperty):
    last_edited_time: str = None
    type: str = "last_edited_time"

    def __str__(self):
        return str(self.last_edited_time)


@dataclass
class PagePropertyLastEditedBy(PageProperty):
    last_edited_by: User = field(default_factory=User)
    type: str = "last_edited_by"

    def __str__(self):
        return str(self.last_edited_by.name) if self.last_edited_by else ""


class PagePropertyFactory:
    """
    This class is a helper to create the proper PageProperty type automaticaly.
    """

    __rel_map = {
        "title": PagePropertyTitle,
        "rich_text": PagePropertyText,
        "number": PagePropertyNumber,
        "select": PagePropertySelect,
        "multi_select": PagePropertyMultiSelect,
        "date": PagePropertyDate,
        "people": PagePropertyPeople,
        "files": PagePropertyFiles,
        "checkbox": PagePropertyCheckbox,
        "url": PagePropertyUrl,
        "email": PagePropertyEmail,
        "phone_number": PagePropertyPhoneNumber,
        "formula": PagePropertyFormula,
        "relation": PagePropertyRelation,
        "rollup": PagePropertyRollup,
        "created_by": PagePropertyCreatedBy,
        "created_time": PagePropertyCreatedTime,
        "last_edited_time": PagePropertyLastEditedTime,
        "last_edited_by": PagePropertyLastEditedBy,
    }

    @staticmethod
    def new(data):
        data_type = data.get("type")
        if data_type and data_type in PagePropertyFactory.__rel_map:
            return from_dict(
                data_class=PagePropertyFactory.__rel_map[data_type], data=data
            )
        else:
            raise Exception(f'PageProperty "{data_type}" not implemented yet.')

    @staticmethod
    def new_default(type: str, payload=any, color=None):
        if type in ["select", "multi_select"]:
            return PagePropertyFactory.__rel_map[type].new(payload, color=color)
        else:
            return PagePropertyFactory.__rel_map[type].new(payload)


@dataclass
class Page(__BaseDataClass):
    properties: object
    parent: Parent
    blocks: Optional[List["Block"]] = field(default_factory=list)
    archived: bool = False
    # icon: Optional[Emoji] = None  # TODO: Handle File
    cover: Optional[File] = None
    created_time: str = None
    last_edited_time: str = None
    url: str = None
    object: str = "page"
    id: str = None

    """
    __post_init__ is called right after the object is created.

    We use it here to keep self.properties as a Dictionnary but we
    want to convert values as Data Classes instances.
    """

    def __post_init__(self):
        for k in self.properties:
            self.properties[k] = PagePropertyFactory.new(self.properties[k])

    def __repr__(self):
        try:
            display(self.df())  # noqa: F821
            return ""
        except:  # noqa: E722
            return str(self)

    def get_blocks(self):
        self.blocks = self.notion.blocks.children(self.id)
        return self.blocks

    def duplicate(self):
        page = deepcopy(self)
        page.id = None
        page.parent.type = None

        to_delete = []
        for col_name in page.properties:
            prop = page.properties[col_name]
            if prop.type in [
                "last_edited_time",
                "created_time",
                "created_by",
                "last_edited_by",
            ]:
                to_delete.append(col_name)
            elif getattr(prop, prop.type) is None:
                to_delete.append(col_name)

        for col_name in to_delete:
            del page.properties[col_name]

        block_tree = BlockTree(self.notion.blocks.get(self.id))
        new_page = page.create()
        block_tree.duplicate(new_page.id)

        return new_page

    def df(self, pivot=False):
        copied = deepcopy(self.properties)
        if pivot is False:
            d = []
            for i in copied:
                p = copied[i]

                d.append({"Name": i, "Type": p.type, "Value": str(p)})
            return pd.DataFrame(self.notion.to_dict(d))
        else:
            frame = {}
            for i in copied:
                p = copied[i]
                frame[i] = str(p)
            return pd.DataFrame(self.notion.to_dict([frame]))

    @classmethod
    def new(cls, page_id: str = None, database_id: str = None):
        parent = None
        if page_id:
            parent = Parent.new_page_parent(page_id=page_id)
        elif database_id:
            parent = Parent.new_database_parent(database_id=database_id)
        else:
            parent = Parent.new_workspace_parent()
        #  For some reasons, not explained in Notion API Documentation, when creating a new page
        # the property 'type' of a Parent object must be omitted.
        parent.type = None

        return cls(parent=parent, properties={})

    def add_block(self, block_type: str):
        new_block = Block.new(type=block_type)
        new_block = new_block.append_to(page_id=self.id)
        self.get_blocks()
        return new_block

    def update(self):
        ret = self.notion.pages.update(self)
        blocks_to_add = []
        for block in self.blocks:
            if not block.id:
                blocks_to_add.append(block)

        if len(blocks_to_add) > 0:
            self.notion.blocks.append(self.id, blocks_to_add)

        return ret

    def create(self):
        return self.notion.pages.create_from_page(self)

    """
    Properties setters
    """

    def __generic_property_setter(self, type: str, column_name, column_value):
        if column_name not in self.properties and self.parent.type == "database_id":
            db = self.notion.databases.retrieve(database_id=self.parent.database_id)
            db.add_property(column_name, type)
            db.update()
        if type in ["select", "multi_select"]:
            db = self.notion.databases.retrieve(database_id=self.parent.database_id)
            select_option = db.properties[column_name].get_option_by_name(column_value)
            color = None
            if select_option:
                color = select_option.color
            self.properties[column_name] = PagePropertyFactory.new_default(
                type, column_value, color=color
            )
        else:
            self.properties[column_name] = PagePropertyFactory.new_default(
                type, column_value
            )
        return self.properties[column_name]

    def title(self, *k):
        return self.__generic_property_setter("title", *k)

    def rich_text(self, *k):
        return self.__generic_property_setter("rich_text", *k)

    def number(self, *k):
        return self.__generic_property_setter("number", *k)

    def select(self, *k):
        return self.__generic_property_setter("select", *k)

    def multi_select(self, *k):
        return self.__generic_property_setter("multi_select", *k)

    def date(self, *k):
        return self.__generic_property_setter("date", *k)

    def people(self, *k):
        return self.__generic_property_setter("people", *k)

    def checkbox(self, *k):
        return self.__generic_property_setter("checkbox", *k)

    def link(self, *k):
        return self.__generic_property_setter("url", *k)

    def email(self, *k):
        return self.__generic_property_setter("email", *k)

    def phone_number(self, *k):
        return self.__generic_property_setter("phone_number", *k)

    """
    Block setters
    """

    def __generic_block_setter(self, type: str, payload):
        new_block = BlockTypeFactory.new_default(type, payload)
        self.blocks.append(new_block)
        return new_block

    def paragraph(self, *k):
        return self.__generic_block_setter("paragraph", *k)

    def heading_1(self, *k):
        return self.__generic_block_setter("heading_1", *k)

    def heading_2(self, *k):
        return self.__generic_block_setter("heading_2", *k)

    def heading_3(self, *k):
        return self.__generic_block_setter("heading_3", *k)

    def bulleted_list_item(self, *k):
        return self.__generic_block_setter("bulleted_list_item", *k)

    def numbered_list_item(self, *k):
        return self.__generic_block_setter("numbered_list_item", *k)

    def to_do(self, *k):
        return self.__generic_block_setter("to_do", *k)

    def toggle(self, *k):
        return self.__generic_block_setter("toggle", *k)

    def code(self, *k):
        return self.__generic_block_setter("code", *k)

    def embed(self, *k):
        return self.__generic_block_setter("embed", *k)

    def image(self, *k):
        return self.__generic_block_setter("image", *k)

    def video(self, *k):
        return self.__generic_block_setter("video", *k)

    def file(self, *k):
        return self.__generic_block_setter("file", *k)

    def pdf(self, *k):
        return self.__generic_block_setter("pdf", *k)

    def bookmark(self, *k):
        return self.__generic_block_setter("bookmark", *k)

    def equation(self, *k):
        return self.__generic_block_setter("equation", *k)

    def divider(self, *k):
        return self.__generic_block_setter("divider", *k)

    def table_of_contents(self, *k):
        return self.__generic_block_setter("table_of_contents", *k)

    def child_database(self, *k):
        return self.__generic_block_setter("child_database", *k)


"""
Block and Blocks properties.
"""


@dataclass
class Block(__BaseDataClass):
    type: str
    paragraph: Optional["BlockParagraph"] = None
    heading_1: Optional["BlockHeadingOne"] = None
    heading_2: Optional["BlockHeadingTwo"] = None
    heading_3: Optional["BlockHeadingThree"] = None
    callout: Optional["BlockCallout"] = None
    quote: Optional["BlockQuote"] = None
    bulleted_list_item: Optional["BlockBulletedListItem"] = None
    numbered_list_item: Optional["BlockNumberedListItem"] = None
    to_do: Optional["BlockToDo"] = None
    toggle: Optional["BlockToggle"] = None
    code: Optional["BlockCode"] = None
    child_page: Optional["BlockChildPage"] = None
    child_database: Optional["BlockChildDatabase"] = None
    embed: Optional["BlockEmbed"] = None
    image: Optional["File"] = None
    video: Optional["File"] = None
    file: Optional["File"] = None
    pdf: Optional["File"] = None
    bookmark: Optional["BlockBookmark"] = None
    equation: Optional["BlockEquation"] = None
    divider: Optional[dict] = None
    table_of_contents: Optional[dict] = None
    has_children: bool = False
    archived: bool = False
    created_time: str = None
    last_edited_time: str = None
    object: str = "block"
    id: str = None

    @classmethod
    def new(cls, type: str, prop: any = None):
        if prop:
            return from_dict(data_class=cls, data={"type": type, type: prop})
        return from_dict(data_class=cls, data={"type": type, type: {}})

    # def __post_init__(self):
    #     if self.has_children is True:
    #         self.children = self.notion.blocks.children(block_id=self.id)

    # TODO: To delete
    # def __post_init__(self):
    #   setattr(self, self.type, BlockTypeFactory.new(self))

    def append_to(self, page_id):
        return self.notion.blocks.append(page_id, self)

    def update(self):
        return self.notion.blocks.update(self)

    def delete(self):
        return self.notion.blocks.delete(self.id)


@dataclass
class BlockParagraph(__BaseDataClass):
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)

    def add(self, content):
        self.text.append(RichText.new_text(content=content))

    @classmethod
    def new(cls, content: str) -> "BlockParagraph":
        return cls(children=[], text=[RichText.new(content)])


@dataclass
class BlockHeadingOne(__BaseDataClass):
    text: List[RichText] = field(default_factory=list)

    @classmethod
    def new(cls, content: str) -> "BlockHeadingOne":
        return cls(text=[RichText.new(content)])


@dataclass
class BlockHeadingTwo(__BaseDataClass):
    text: List[RichText] = field(default_factory=list)

    @classmethod
    def new(cls, content: str) -> "BlockHeadingTwo":
        return cls(text=[RichText.new(content)])


@dataclass
class BlockHeadingThree(__BaseDataClass):
    text: List[RichText] = field(default_factory=list)

    @classmethod
    def new(cls, content: str) -> "BlockHeadingThree":
        return cls(text=[RichText.new(content)])


@dataclass
class BlockCallout(__BaseDataClass):
    icon: Optional[Union[File, Emoji]]
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)

    @classmethod
    def new(cls, content: str) -> "BlockCallout":
        return cls(icon=None, children=[], text=[RichText.new(content)])


@dataclass
class BlockQuote(__BaseDataClass):
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)

    @classmethod
    def new(cls, content: str) -> "BlockQuote":
        return cls(children=[], text=[RichText.new(content)])


@dataclass
class BlockBulletedListItem(__BaseDataClass):
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)

    @classmethod
    def new(cls, content: str) -> "BlockBulletedListItem":
        return cls(children=[], text=[RichText.new(content)])


@dataclass
class BlockNumberedListItem(__BaseDataClass):
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)

    @classmethod
    def new(cls, content: str) -> "BlockNumberedListItem":
        return cls(children=[], text=[RichText.new(content)])


@dataclass
class BlockToDo(__BaseDataClass):
    checked: Optional[bool]
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)

    @classmethod
    def new(cls, content: str) -> "BlockToDo":
        return cls(checked=False, children=[], text=[RichText.new(content)])


@dataclass
class BlockToggle(__BaseDataClass):
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)

    @classmethod
    def new(cls, content: str) -> "BlockToggle":
        return cls(children=[], text=[RichText.new(content)])


@dataclass
class BlockCode(__BaseDataClass):
    language: Optional[str] = "plain text"
    text: List[RichText] = field(default_factory=list)

    @classmethod
    def new(cls, content: str) -> "BlockCode":
        return cls(text=[RichText.new(content)])


@dataclass
class BlockChildPage(__BaseDataClass):
    title: Optional[str] = "New page"


@dataclass
class BlockChildDatabase(__BaseDataClass):
    title: Optional[str] = ""

    @classmethod
    def new(cls, title: str) -> "BlockChildDatabase":
        return cls(title=title)


@dataclass
class BlockEmbed(__BaseDataClass):
    url: Optional[str] = ""

    @classmethod
    def new(cls, url: str) -> "BlockEmbed":
        return cls(url=url)


@dataclass
class BlockImage(__BaseDataClass):
    image: Optional[File] = field(default_factory=File)

    @classmethod
    def new(cls, url: str) -> "BlockImage":
        return cls(image=File.new(url))


@dataclass
class BlockVideo(__BaseDataClass):
    video: Optional[File] = field(default_factory=File)

    @classmethod
    def new(cls, url: str) -> "BlockVideo":
        return cls(video=File.new(url))


@dataclass
class BlockFile(__BaseDataClass):
    file: Optional[File] = field(default_factory=File)

    @classmethod
    def new(cls, url: str) -> "BlockFile":
        return cls(file=File.new(url))


@dataclass
class BlockPdf(__BaseDataClass):
    pdf: Optional[File] = field(default_factory=File)

    @classmethod
    def new(cls, url: str) -> "BlockPdf":
        return cls(pdf=File.new(url))


@dataclass
class BlockBookmark(__BaseDataClass):
    caption: List[RichText] = field(default_factory=list)
    url: str = ""

    @classmethod
    def new(cls, content: str) -> "BlockBookmark":
        return cls(caption=[RichText.new(content)])


@dataclass
class BlockEquation(__BaseDataClass):
    expression: str = ""

    @classmethod
    def new(cls, expression: str) -> "BlockEquation":
        return cls(expression=expression)


@dataclass
class BlockDivider(__BaseDataClass):
    divider: object = field(default_factory=dict)

    @classmethod
    def new(cls) -> "BlockDivider":
        return cls()


@dataclass
class BlockTableOfContents(__BaseDataClass):
    table_of_contents: object = field(default_factory=dict)

    @classmethod
    def new(cls) -> "BlockTableOfContents":
        return cls()


class BlockTypes(Enum):
    paragraph = "paragraph"
    heading_1 = "heading_1"
    heading_2 = "heading_2"
    heading_3 = "heading_3"
    callout = "callout"
    quote = "quote"
    bulleted_list_item = "bulleted_list_item"
    numbered_list_item = "numbered_list_item"
    to_do = "to_do"
    toggle = "toggle"
    code = "code"
    child_page = "child_page"
    child_database = "child_database"
    embed = "embed"
    image = "image"
    video = "video"
    file = "file"
    pdf = "pdf"
    bookmark = "bookmark"
    equation = "equation"
    divider = "divider"
    table_of_contents = "table_of_contents"
    unsupported = "unsupported"


class BlockTypeFactory:
    """
    This class is a helper to create the proper BlockType automaticaly.
    """

    __rel_map = {
        "paragraph": BlockParagraph,
        "heading_1": BlockHeadingOne,
        "heading_2": BlockHeadingTwo,
        "heading_3": BlockHeadingThree,
        "callout": BlockCallout,  # Seems to be missing documentation as of 21/10/17
        "quote": BlockQuote,  # Seems to be missing documentation as of 21/10/17
        "bulleted_list_item": BlockBulletedListItem,
        "numbered_list_item": BlockNumberedListItem,
        "to_do": BlockToDo,
        "toggle": BlockToggle,
        "code": BlockCode,  # Seems to be missing documentation as of 21/10/17
        "child_page": BlockChildPage,
        "child_database": BlockChildDatabase,
        "embed": BlockEmbed,
        "image": File,
        # "video": BlockVideo,
        "video": File,
        "file": File,
        "pdf": File,
        "bookmark": BlockBookmark,
        "equation": BlockEquation,  # Seems to be missing documentation as of 21/10/17
        "divider": dict,  # Seems to be missing documentation as of 21/10/17
        "table_of_contents": dict,  # Seems to be missing documentation as of 21/10/17
        # "unsupported": BlockUnsupported
    }

    # @staticmethod
    # def new(data):
    #    data_type = getattr(data, 'type')
    #    if data_type and data_type in BlockTypeFactory.__rel_map:
    #        return from_dict(data_class=BlockTypeFactory.__rel_map[data_type], data=getattr(data, data_type))
    #    else:
    #        raise Exception(f'BlockType "{data_type}" not implemented yet.')

    @staticmethod
    def new_default(type: str, payload: any):
        if type in ["divider", "table_of_contents"]:
            prop = BlockTypeFactory.__rel_map[type].new()
        else:
            prop = BlockTypeFactory.__rel_map[type].new(payload)
        return Block.new(type, prop)


"""Dataclass json encoder"""


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


def del_none(d):
    """
    Delete keys with the value ``None`` in a dictionary, recursively.

    This alters the input so you may wish to ``copy`` the dict first.
    """
    # For Python 3, write `list(d.items())`; `d.items()` won’t work
    # For Python 2, write `d.items()`; `d.iteritems()` won’t work
    if type(d) is dict:
        for key, value in list(d.items()):
            if value is None:
                del d[key]
            elif isinstance(value, dict) or isinstance(value, list):
                del_none(value)
    elif type(d) is list:
        for e in d:
            del_none(e)
    return d  # For convenience


class BlockTree:
    block: Block = None
    children: List["BlockTree"] = None

    def __init__(self, block: Block):
        self.block = block
        if self.block.has_children is True:
            self.children = []
            blocks = Notion.instance().blocks.children(self.block.id)
            for b in blocks:
                self.children.append(BlockTree(b))

    def copy(self):
        copied_block = deepcopy(self.block)
        copied_block.id = None
        copied_block.created_time = None
        copied_block.last_edited_time = None
        return copied_block

    def duplicate_children(self, parent_id):
        if self.children is not None:
            to_append = []
            for child in self.children:
                to_append.append(child.copy())
            new_blocks = Notion.instance().block.append(parent_id, to_append)
            count = 0
            for child in self.children:
                child.duplicate_children(new_blocks[count].id)
                count = count + 1

    def duplicate(self, parent_id):
        if self.children is not None:
            to_append = []
            for child in self.children:
                to_append.append(child.copy())
            new_blocks = Notion.instance().block.append(parent_id, to_append)

            threads = []
            count = 0
            for child in self.children:
                x = threading.Thread(
                    target=child.duplicate_children, args=(new_blocks[count].id,)
                )
                threads.append(x)
                x.start()
                count = count + 1

            for t in threads:
                t.join()
