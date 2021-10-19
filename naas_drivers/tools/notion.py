from naas_drivers.driver import InDriver, OutDriver
from notion_client import Client
import dataclasses
from dataclasses import dataclass, field, make_dataclass
from dacite import from_dict
from typing import List, Optional, Union
from numbers import Number
from enum import Enum
import json
import logging
from copy import deepcopy

VERSION = "2021-08-16"

global notion_instance

class Notion(InDriver, OutDriver):
    __client = None

    def __init__(self):
        """
        Classes / Methods to query notion
        """
        self.databases = self.Databases(self)
        self.pages = self.Pages(self)
        self.blocks = self.Blocks(self)
        
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
    
    class __InnerBase():
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
        
        def query(self, database_id, query = {}):
            ret = []
            results = self.client.databases.query(database_id=database_id, **query).get('results')
            for r in results:
                ret.append(from_dict(data_class=Page, data=r))
            return ret
    
        def create(self, db):
            payload = self.parent.to_dict(db)
            return self.__from_dict(self.client.databases.create(**payload))
    
        def update(self, db):
            payload = self.parent.to_dict(db)
            return self.__from_dict(self.client.databases.update(database_id=db.id, **payload))
    
        def retrieve(self, database_id):
            raw = self.client.databases.retrieve(database_id)
            return self.__from_dict(raw)

    class Pages(__InnerBase):
        
        def __from_dict(self, data):
            return from_dict(data_class=Page, data=data)
        
        def create(self, page):
            payload = self.parent.to_dict(page)
            return self.__from_dict(self.client.pages.create(**payload))
    
        def retrieve(self, page_id : str):
            page = self.client.pages.retrieve(page_id=page_id)
            return self.__from_dict(page)

        def update(self, page): #  TODO: Improve readability.
            copied = deepcopy(page)
            filtered_properties = {}
            for p in copied.properties:
                prop = copied.properties[p]
                if prop.type not in ['last_edited_by', 'last_edited_time', 'created_by', 'created_time'] and getattr(prop, prop.type) != None:
                    filtered_properties[p] = prop
            copied.properties = filtered_properties
            payload = self.parent.to_dict(copied)
            print(copied.properties)
            return self.__from_dict(self.client.pages.update(page_id=copied.id, **payload))
    
    class Blocks(__InnerBase):
        
        def __from_dict(self, data):
            return from_dict(data_class=Block, data=data)
    
        def retrieve(self, block_id: str):
            block = self.client.blocks.retrieve(block_id=block_id)
            return self.__from_dict(block)
        
        def children(self, block_id : str):
            ret = []
            for r in self.client.blocks.children.list(block_id=block_id).get('results'):
                ret.append(self.__from_dict(r))
            return ret
        
        def append(self, block_id, blocks):
            if type(blocks) != list:
                blocks = [blocks]
            payload = self.parent.to_dict(blocks)
            print(payload)
            ret = []
            for b in self.client.blocks.children.append(block_id=block_id, children=payload).get('results'):
                ret.append(self.__from_dict(b))
            return ret
        
        def update(self, block):
            payload = self.parent.to_dict(block)
            return self.__from_dict(self.client.blocks.update(block_id=block.id, **payload))
        
        def delete(self, block_id):
            return self.__from_dict(self.client.blocks.delete(block_id=f'/{block_id}')) # TODO: Remove '/' when PR is merged.
        
        

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
    url: Optional[str] = ''
    
@dataclass
class File(__BaseDataClass):
    url: Optional[str]
    expiry_time: Optional[str]
    name: Optional[str]
    external: Optional[FileExternal] = field(default_factory=FileExternal)
    type: str = 'external'

@dataclass
class Emoji(__BaseDataClass):
    emoji: str
    type: str = 'emoji'

@dataclass
class Parent(__BaseDataClass):
    type: str
    workspace: Optional[bool] = None
    page_id: Optional[str] = None
    database_id : Optional[str] = None
    
    @classmethod
    def new_page_parent(cls, page_id):
        return cls(type='page_id', page_id=page_id)
        

    @classmethod
    def new_workspace_parent(cls):
        return cls(type='workspace', workspace=True)
    
    @classmethod
    def new_database_parent(cls, database_id):
        return cls(type='database_id', database_id=database_id)
    
@dataclass
class Link(__BaseDataClass):
    type: str
    url: str

@dataclass
class Text(__BaseDataClass):
    content: str
    link: Optional[Link] = None

@dataclass
class Annotation(__BaseDataClass):
    bold: bool = False
    italic: bool = False
    strikethrough: bool = False
    underline: bool = False
    code: bool = False
    color: str = 'default'

@dataclass
class Person(__BaseDataClass):
    email: str

@dataclass
class User(__BaseDataClass):
    object: str
    id: str
    type: Optional[str]
    name: Optional[str]
    avatar_url: Optional[str]
    person: Optional[Person]
    bot: Optional[object]
    
    def __post_init__(self):
        if self.type == 'person':
            self.person = Person('')
        if self.type == 'bot':
            self.bot = {}

@dataclass
class Date(__BaseDataClass):
    start: str
    end: Optional[str]

@dataclass
class Equation(__BaseDataClass):
    expression: str
        
@dataclass
class RichText(__BaseDataClass):
    plain_text: str
    href: Optional[str] = None
    annotations: Annotation = field(default_factory=Annotation)
    type: str = 'text'
    text: Optional[Text] = None
    user: Optional[User] = None
    page: Optional[object] = None
    database: Optional[object] = None
    date: Optional[Date] = None
    equation: Optional[Equation] = None
        
    @classmethod
    def new_text(cls, content):
        return cls(plain_text=content, text=Text(content=content))

"""
Database and Database properties

This section is containing all classes related to Properties (https://developers.notion.com/reference/database#property-object)
"""

@dataclass
class Property(__BaseDataClass):
    id: Optional[str]
    type: str # TODO: Consider using enum to improve typing.


@dataclass
class DatabaseProperty(Property):
    name: str
    
    
@dataclass
class DatabasePropertyTitle(DatabaseProperty):
    title: object = field(default_factory=dict)
    

@dataclass
class DatabasePropertyText(DatabaseProperty):
    rich_text: object = field(default_factory=dict)

@dataclass
class DatabasePropertyNumber_Configuration:
    format: str = 'number'

@dataclass
class DatabasePropertyNumber(DatabaseProperty):
    number: DatabasePropertyNumber_Configuration = field(default_factory=dict)

@dataclass
class DatabasePropertySelect_Configuration_Option:
    name: str
    id: str
    color: str # TODO: enum
        
@dataclass
class DatabasePropertySelect_Configuration:
    options: List[DatabasePropertySelect_Configuration_Option] = field(default_factory=list)
        
@dataclass
class DatabasePropertySelect(DatabaseProperty):
    select: DatabasePropertySelect_Configuration = field(default_factory=DatabasePropertySelect_Configuration)

@dataclass
class DatabasePropertyMultiSelect(DatabaseProperty):
    multi_select: DatabasePropertySelect_Configuration = field(default_factory=DatabasePropertySelect_Configuration) # TODO: Create own childs? Not reuse DatabasePropertySelect_Configuration class.


@dataclass
class DatabasePropertyDate(DatabaseProperty):
    date: object = field(default_factory=dict)
    
@dataclass
class DatabasePropertyPeople(DatabaseProperty):
    people: object = field(default_factory=dict)

@dataclass
class DatabasePropertyFiles(DatabaseProperty):
    files: object = field(default_factory=dict)

@dataclass
class DatabasePropertyCheckbox(DatabaseProperty):
    checkbox: object = field(default_factory=dict)

@dataclass
class DatabasePropertyUrl(DatabaseProperty):
    url: object = field(default_factory=dict)
    
@dataclass
class DatabasePropertyEmail(DatabaseProperty):
    email: object = field(default_factory=dict)
        
@dataclass
class DatabasePropertyPhoneNumber(DatabaseProperty):
    phone_number: object = field(default_factory=dict)

@dataclass
class DatabasePropertyFormula_Configuration:
    expression: str = ''
        
@dataclass
class DatabasePropertyFormula(DatabaseProperty):
    formula: DatabasePropertyFormula_Configuration = field(default_factory=DatabasePropertyFormula_Configuration)

@dataclass
class DatabasePropertyRelation_Configuration:
    database_id: str
    synced_property_name: Optional[str]
    synced_property_id: Optional[str]
        
@dataclass
class DatabasePropertyRelation(DatabaseProperty):
    relation: DatabasePropertyRelation_Configuration = field(default_factory=DatabasePropertyRelation_Configuration)

@dataclass
class DatabasePropertyRollup_Configuration:
    relation_property_name: str
    relation_property_id: str
    rollup_property_name: str
    rollup_property_id: str
    function: str
    
@dataclass
class DatabasePropertyRollup(DatabaseProperty):
    rollup: DatabasePropertyRollup_Configuration

@dataclass
class DatabasePropertyCreatedBy(DatabaseProperty):
    created_by: object = field(default_factory=dict)

@dataclass
class DatabasePropertyCreatedTime(DatabaseProperty):
    created_time: object = field(default_factory=dict)
    
    
@dataclass
class DatabasePropertyLastEditedTime(DatabaseProperty):
    last_edited_time: object = field(default_factory=dict)
        
    
@dataclass
class DatabasePropertyLastEditedBy(DatabaseProperty):
    last_edited_by: object = field(default_factory=dict)
        
class DatabasePropertyFactory():
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
        "last_edited_by": DatabasePropertyLastEditedBy
    }
    
    @staticmethod
    def new(data):
        data_type = data.get('type')
        if data_type and data_type in DatabasePropertyFactory.__rel_map:
            return from_dict(data_class=DatabasePropertyFactory.__rel_map[data_type], data=data)
        else:
            raise Exception(f'DatabaseProperty "{data_type}" not implemented yet.')

            
@dataclass
class Database(__BaseDataClass):
    title: List[RichText]
    properties: object
    parent: Parent
    icon: Optional[Union[File, Emoji]] = None
    cover: Optional[File] = None
    created_time: str = None
    last_edited_time: str = None
    url: str = None
    object: str = 'database'
    id: str = None
    
    """
    __post_init__ is called right after the object is created.
    
    We use it here to keep self.properties as a Dictionnary but we
    want to convert values as Data Classes instances.
    """
    def __post_init__(self):
        for k in self.properties:
            self.properties[k] = DatabasePropertyFactory.new(self.properties[k])
    
    @classmethod
    def new(cls, title : str, page_id:str = None):
        parent = None
        if page_id:
            parent = Parent.new_page_parent(page_id=page_id)
        else:
            parent = Parent.new_workspace_parent()
            
        return cls(title=[RichText.new_text(content=title)], parent=parent, properties={})
    
    @classmethod
    def from_dict(cls, data):
        return from_dict(data_class=cls, data=data)
    
    def set_emoji_icon(self, data): # TODO: Fix, seems like there is an issue with notion-client / httpx maybe.
        self.icon = Emoji(data)
    
    def add_property(self, col_name, type_name):
        data = {
            "type": type_name,
            "name": col_name,
            "id": ''
        }
        self.properties[col_name] = DatabasePropertyFactory.new(data)
    
    def query(self, query = {}):
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
    title: Optional[List[RichText]]
        
    def set_text(self, content):
        self.title = [RichText.new_text(content)]

@dataclass
class PagePropertyText(PageProperty):
    rich_text: Optional[List[RichText]]

@dataclass
class PagePropertyNumber(PageProperty):
    number: Optional[Number]

@dataclass
class PagePropertySelect_Value:
    name: str
    id : str = None
    color: str = 'default'
    
@dataclass
class PagePropertySelect(PageProperty):
    select: Optional[PagePropertySelect_Value] = field(default_factory=PagePropertySelect_Value)

@dataclass
class PagePropertyMultiSelect(PageProperty):
    multi_select: Optional[List[PagePropertySelect_Value]] = field(default_factory=list) # TODO: Create own Value type.

@dataclass
class PagePropertyDate_Value:
    start: str
    end: Optional[str]
        
@dataclass
class PagePropertyDate(PageProperty):
    date: Optional[PagePropertyDate_Value] = field(default_factory=PagePropertyDate_Value)

@dataclass
class PagePropertyFormula(PageProperty):
    type: str
    string: Optional[str]
    number: Optional[Number]
    boolean: Optional[bool]
    date: Optional[PagePropertyDate_Value]

@dataclass
class PagePropertyRelation(PageProperty):
    relation : List[object] = field(default_factory=list)

@dataclass
class PagePropertyRollup(PageProperty):
    type: str
    number: Optional[Number]
    date: Optional[PagePropertyDate_Value]
    array: Optional[List[object]]

@dataclass
class PagePropertyPeople(PageProperty):
    people: Optional[List[User]]
    
@dataclass
class PagePropertyFiles(PageProperty):
    files: Optional[List[File]]

@dataclass
class PagePropertyCheckbox(PageProperty):
    checkbox: bool

@dataclass
class PagePropertyUrl(PageProperty):
    url: Optional[str]

@dataclass
class PagePropertyEmail(PageProperty):
    email: Optional[str]

@dataclass
class PagePropertyPhoneNumber(PageProperty):
    phone_number: Optional[str]
         
@dataclass
class PagePropertyCreatedBy(PageProperty):
    created_by: User
            
@dataclass
class PagePropertyCreatedTime(PageProperty):
    created_time: str

@dataclass
class PagePropertyLastEditedTime(PageProperty):
    last_edited_time: str

@dataclass
class PagePropertyLastEditedBy(PageProperty):
    last_edited_by: User
        
class PagePropertyFactory():
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
        "last_edited_by": PagePropertyLastEditedBy
    }
    
    @staticmethod
    def new(data):
        data_type = data.get('type')
        if data_type and data_type in PagePropertyFactory.__rel_map:
            return from_dict(data_class=PagePropertyFactory.__rel_map[data_type], data=data)
        else:
            raise Exception(f'PageProperty "{data_type}" not implemented yet.')


@dataclass
class Page(__BaseDataClass):
    properties: object
    parent: Parent
    blocks : Optional[List['Block']] = None
    archived : bool = False
    icon: Optional[Union[File, Emoji]] = None
    cover: Optional[File] = None
    created_time: str = None
    last_edited_time: str = None
    url: str = None
    object: str = 'page'
    id: str = None

    """
    __post_init__ is called right after the object is created.
    
    We use it here to keep self.properties as a Dictionnary but we
    want to convert values as Data Classes instances.
    """
    def __post_init__(self):
        for k in self.properties:
            self.properties[k] = PagePropertyFactory.new(self.properties[k])
        #self.get_blocks()
    
    def get_blocks(self):
        self.blocks = self.notion.blocks.children(self.id)
        return self.blocks

    @classmethod
    def new(cls, page_id:str = None, database_id : str = None):
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
    
    def add_block(self, block_type:str):
        new_block = Block.new(type=block_type)
        new_block = new_block.append_to(page_id=self.id)
        self.get_blocks()
        return new_block
        
    
    def update(self):
        return self.notion.pages.update(self)
    
    def create(self):
        return self.notion.pages.create(self)

    
"""
Block and Blocks properties.
"""

@dataclass
class Block(__BaseDataClass):
    type : str
    paragraph: Optional['BlockParagraph'] = None
    heading_1: Optional['BlockHeadingOne'] = None
    heading_2: Optional['BlockHeadingTwo'] = None
    heading_3: Optional['BlockHeadingThree'] = None
    callout: Optional['BlockCallout'] = None
    quote: Optional['BlockQuote'] = None
    bulleted_list_item: Optional['BlockBulletedListItem'] = None
    numbered_list_item: Optional['BlockNumberedListItem'] = None
    to_do: Optional['BlockToDo'] = None
    toggle: Optional['BlockToggle'] = None
    code: Optional['BlockCode'] = None
    child_page: Optional['BlockChildPage'] = None
    child_database: Optional['BlockChildDatabase'] = None
    embed: Optional['BlockEmbed'] = None
    image: Optional['File'] = None
    video: Optional['File'] = None
    file: Optional['File'] = None
    pdf: Optional['File'] = None
    bookmark: Optional['BlockBookmark'] = None
    equation: Optional['BlockEquation'] = None
    divider: Optional[dict] = None
    table_of_contents: Optional[dict] = None
    has_children : bool = False
    archived : bool = False
    created_time: str = None
    last_edited_time: str = None
    object: str = 'block'
    id: str = None
    
    @classmethod
    def new(cls, type: str):
        return from_dict(data_class=cls, data={"type": type, type: {}})
    
    # TODO: To delete
    #def __post_init__(self):
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

@dataclass
class BlockHeadingOne(__BaseDataClass):
    text: List[RichText] = field(default_factory=list)

@dataclass
class BlockHeadingTwo(__BaseDataClass):
    text: List[RichText] = field(default_factory=list)

@dataclass
class BlockHeadingThree(__BaseDataClass):
    text: List[RichText] = field(default_factory=list)

@dataclass
class BlockCallout(__BaseDataClass):
    icon: Optional[Union[File, Emoji]]
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)


@dataclass
class BlockQuote(__BaseDataClass):
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)


@dataclass
class BlockBulletedListItem(__BaseDataClass):
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)

@dataclass
class BlockNumberedListItem(__BaseDataClass):
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)

@dataclass
class BlockToDo(__BaseDataClass):
    checked : Optional[bool]
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)


@dataclass
class BlockToggle(__BaseDataClass):
    children: Optional[List[Block]]
    text: List[RichText] = field(default_factory=list)

@dataclass
class BlockCode(__BaseDataClass):
    language: Optional[str] = 'plain text'
    text: List[RichText] = field(default_factory=list)

@dataclass
class BlockChildPage(__BaseDataClass):
    title: Optional[str] = 'New page'
        
@dataclass
class BlockChildDatabase(__BaseDataClass):
    title: Optional[str] = ''

@dataclass
class BlockEmbed(__BaseDataClass):
    url: Optional[str] = ''

@dataclass
class BlockImage(__BaseDataClass):
    image: Optional[File] = field(default_factory=File)

@dataclass
class BlockVideo(__BaseDataClass):
    video: Optional[File] = field(default_factory=File)

@dataclass
class BlockFile(__BaseDataClass):
    file: Optional[File] = field(default_factory=File)

@dataclass
class BlockPdf(__BaseDataClass):
    pdf: Optional[File] = field(default_factory=File)
        
@dataclass
class BlockBookmark(__BaseDataClass):
    caption: List[RichText] = field(default_factory=list)
    url: str = ''

@dataclass
class BlockEquation(__BaseDataClass):
    expression: str = ''

@dataclass
class BlockDivider(__BaseDataClass):
    divider: object = field(default_factory=dict)
        
@dataclass
class BlockTableOfContents(__BaseDataClass):
    table_of_contents: object = field(default_factory=dict)

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
        
# TODO: To delete, never used
class BlockTypeFactory:
    """
    This class is a helper to create the proper BlockType automaticaly.
    """
    
    __rel_map = {
        "paragraph": BlockParagraph,
        "heading_1": BlockHeadingOne,
        "heading_2": BlockHeadingTwo,
        "heading_3": BlockHeadingThree,
        "callout": BlockCallout, # Seems to be missing documentation as of 21/10/17
        "quote": BlockQuote, # Seems to be missing documentation as of 21/10/17
        "bulleted_list_item": BlockBulletedListItem,
        "numbered_list_item": BlockNumberedListItem,
        "to_do": BlockToDo,
        "toggle": BlockToggle,
        "code": BlockCode, # Seems to be missing documentation as of 21/10/17
        "child_page": BlockChildPage,
        "child_database": BlockChildDatabase,
        "embed": BlockEmbed,
        "image": File,
        #"video": BlockVideo,
        "video": File,
        "file": File,
        "pdf": File,
        "bookmark": BlockBookmark,
        "equation": BlockEquation, # Seems to be missing documentation as of 21/10/17
        "divider": dict, # Seems to be missing documentation as of 21/10/17
        "table_of_contents": dict, # Seems to be missing documentation as of 21/10/17
       # "unsupported": BlockUnsupported
    }
    
    @staticmethod
    def new(data):
        data_type = getattr(data, 'type')
        if data_type and data_type in BlockTypeFactory.__rel_map:
            return from_dict(data_class=BlockTypeFactory.__rel_map[data_type], data=getattr(data, data_type))
        else:
            raise Exception(f'BlockType "{data_type}" not implemented yet.')     

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
