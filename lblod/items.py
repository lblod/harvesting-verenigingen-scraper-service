from scrapy import Field, Item
from itemloaders.processors import MapCompose, TakeFirst


class Page(Item):
    url = Field(input_processor=MapCompose(str.strip), output_processor=TakeFirst())
    contents = Field(output_processor=TakeFirst())
    rdo = Field(output_processor=TakeFirst())

    uuid = Field(output_processor=TakeFirst())
    size = Field(output_processor=TakeFirst())
    file_created = Field(output_processor=TakeFirst())
    extension = Field(output_processor=TakeFirst())
    format = Field(output_processor=TakeFirst())
    physical_file_name = Field(output_processor=TakeFirst()) 
    physical_file_path = Field(output_processor=TakeFirst())
