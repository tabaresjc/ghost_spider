# -*- coding: utf-8 -*-

from scrapy.item import Item, Field


class HotelItem(Item):

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


class SalonItem(Item):

  """required fields."""
  # name of place
  page_url = Field()
  name = Field()
  name_kata = Field()
  address = Field()
  routes = Field()
  phone = Field()
  working_hours = Field()
  holydays = Field()
  shop_url = Field()
  credit_cards = Field()
  credit_cards_comment = Field()
  seats = Field()
  stylist = Field()
  parking = Field()
  cut_price = Field()
  prefecture = Field()
  area = Field()
  page_body = Field()


class LocationHotelItem(Item):

  """required fields."""
  # name of place
  page_url = Field()
  name = Field()
  name_kata = Field()
  address = Field()
  routes = Field()
  phone = Field()
  shop_url = Field()
  credit_cards = Field()
  credit_cards_comment = Field()
  prefecture = Field()
  area = Field()
  genre = Field()
  checkin = Field()
  checkout = Field()
  kind = Field()
  votes = Field()
  page_body = Field()


class LocationRestaurantItem(Item):

  """required fields."""
  # name of place
  page_url = Field()
  name = Field()
  name_kata = Field()
  address = Field()
  phone = Field()
  prefecture = Field()
  area = Field()
  genre = Field() # genre from loco
  kind = Field()  # convert genre to latte kind
  page_body = Field()
