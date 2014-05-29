# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class GhostSpiderItem(Item):
  
  """required fields."""
  # name of place
  page_url = Field()
  page_breadcrumbs = Field()
  name = Field()
  place = Field()
  region = Field()
  # phone number
  phone = Field()
  url = Field()

  # popularity
  rating = Field()
  popularity = Field()
