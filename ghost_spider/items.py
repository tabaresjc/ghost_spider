# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class GhostSpiderItem(Item):
  
  """required fields."""
  # name of place
  name = Field()
  name_ja = Field()
  name_es = Field()
  name_za = Field()

  # address of place
  address_area_name = Field()
  address_street = Field()
  address_locality = Field()
  address_locality = Field()
  address_region = Field()
  address_zip = Field()

  address_area_name_ja = Field()
  address_street_ja = Field()
  address_locality_ja = Field()
  address_locality_ja = Field()
  address_region_ja = Field()
  address_zip_ja = Field()

  address_area_name_es = Field()
  address_street_es = Field()
  address_locality_es = Field()
  address_locality_es = Field()
  address_region_es = Field()
  address_zip_es = Field()

  address_za = Field()
  address_area_name_za = Field()
  address_street_za = Field()
  address_locality_za = Field()
  address_locality_za = Field()
  address_region_za = Field()
  address_zip_za = Field()

  # phone number
  phone = Field()
  url = Field()
  amenity = Field()
  amenity_ja = Field()
  amenity_es = Field()
  amenity_za = Field()

  # popularity
  rating = Field()

  page_body = Field()
  page_body_ja = Field()
  page_body_es = Field()
  page_body_za = Field()
