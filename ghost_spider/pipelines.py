# -*- coding: utf-8 -*-

from ghost_spider import helper
from ghost_spider.items import SalonItem, HotelItem, LocationHotelItem, LocationRestaurantItem, LocationAirportItem, LocationBusItem
from elastic import LocationEs, SalonEs, LocationHotelEs, LocationRestaurantEs, LocationAirportEs, LocationBusEs


class SalonPipeline(object):

  """Process & format data after being scrapped from salon page."""

  def process_item(self, item, spider):
    if not isinstance(item, SalonItem):
      return item
    data = SalonEs.get_data(item)
    if data:
      SalonEs.save(data)
    return item


class LocationHotelPipeline(object):

  """Process & format data after being scrapped from hotel page."""

  def process_item(self, item, spider):
    if not isinstance(item, LocationHotelItem):
      return item
    data = LocationHotelEs.get_data(item)
    if data:
      LocationHotelEs.save(data)
    return item


class LocationRestaurantPipeline(object):

  """Process & format data after being scrapped from restaurant page."""

  def process_item(self, item, spider):
    if not isinstance(item, LocationRestaurantItem):
      return item
    data = LocationRestaurantEs.get_data(item)
    if data:
      LocationRestaurantEs.save(data)
      LocationRestaurantEs.refresh()
    return item


class LocationAirportPipeline(object):

  """Process & format data after being scrapped from airports page."""

  def process_item(self, item, spider):
    if not isinstance(item, LocationAirportItem):
      return item
    data = LocationAirportEs.get_data(item)
    if data:
      LocationAirportEs.save(data)
      LocationAirportEs.refresh()
    return item


class LocationBusPipeline(object):

  """Process & format data after being scrapped from bus pages."""

  def process_item(self, item, spider):
    if not isinstance(item, LocationBusItem):
      return item
    data = LocationBusEs.get_data(item)
    if data:
      LocationBusEs.save(data)
      LocationBusEs.refresh()
    return item


class HotelPipeline(object):

  """Process & format data after being scrapped from hotel page (Old)."""

  def process_item(self, item, spider):
    if not isinstance(item, HotelItem):
      return item
    for k, v in item.iteritems():
      if k == 'phone':
        if v and len(v):
          v = helper.SEL_RE_PHONE_NUMBER.findall(v[0])
        item[k] = helper.rev_telephone(v[0] if len(v) else u'')
      elif k == 'page_breadcrumbs':
          if v and len(v):
            item[k] = v[:len(v) - 1] if v else []
          else:
            item[k] = []
      elif k == 'place':
        item[k] = self.clean_place(v)
      else:
        item[k] = helper.clean_lf(v)
    LocationEs.save(self.save_item_to_es(item))
    return item

  def clean_place(self, places):
    new_places = []
    for place in places:
      for k, v in place.iteritems():
        if k == 'amenity':
          place[k] = helper.clean_lf(v, u', ')
        elif k == 'page_body':
          pass
        else:
          place[k] = helper.clean_lf(v)
      new_places.append(place)
    return new_places

  def save_item_to_es(self, item):
    item_es = {}
    item_es['name_low'] = item['name'].lower().strip()
    item_es['rating'] = float(item['rating'] or 0)
    item_es['popularity'] = float(item['popularity'] or 0)
    item_es['page_url'] = item['page_url'].lower()
    item_es['page_breadcrumbs'] = item['page_breadcrumbs']
    item_es['phone'] = item['phone']
    item_es['area1'] = item['page_breadcrumbs'][0].strip() if len(item['page_breadcrumbs']) > 0 else u''
    item_es['area2'] = item['page_breadcrumbs'][1].strip() if len(item['page_breadcrumbs']) > 1 else u''
    state = helper.CLEAN_STATE.findall(item_es['area2'])
    if state and len(state):
      item_es['area2'] = state[0].strip()
    item_es['area3'] = item['page_breadcrumbs'][2].strip() if len(item['page_breadcrumbs']) > 2 else u''
    item_es['area4'] = item['page_breadcrumbs'][3].strip() if len(item['page_breadcrumbs']) > 3 else u''
    item_es['area5'] = item['page_breadcrumbs'][4].strip() if len(item['page_breadcrumbs']) > 4 else u''
    item_es['region'] = item['region'].strip()
    item_es['place'] = item['place']
    item_es['id'] = LocationEs.get_hash(item_es['page_url'])
    return item_es
