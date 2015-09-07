# -*- coding: utf-8 -*-

from scrapy.selector import Selector
from scrapy.http import Request
from ghost_spider.items import LocationBusItem
from ghost_spider.helper import AirportSelectors
from ghost_spider.data import BUS_STOP_FILES
from ghost_spider.elastic import LocationBusEs
from ghost_spider.util import BaseSpider
from ghost_spider.lib.geolocationlib import GeoLocation
from ghost_spider import helper


class LocationBusSpider(BaseSpider):
  name = "location_bus"
  allowed_domains = ["localhost"]
  base_url = None
  start_urls = BUS_STOP_FILES
  count = 0
  total = 0
  scan_mode = False

  xml_namespaces = [
    ('ksj', 'http://nlftp.mlit.go.jp/ksj/schemas/ksj-app'),
    ('gml', 'http://www.opengis.net/gml/3.2'),
    ('xlink', 'http://www.w3.org/1999/xlink'),
    ('xsi', 'http://www.w3.org/2001/XMLSchema-instance'),
    ('jps', 'http://www.gsi.go.jp/GIS/jpgis/standardSchemas')
  ]

  def __init__(self, name=None, **kwargs):
    self.log_message('*-' * 50)
    self.log_message('Starting...')
    self.count = 0
    self.total = 0
    super(LocationBusSpider, self).__init__(self.name, **kwargs)

  def parse(self, response):
    sel = LocationBusSpider.get_selector(response)
    places = sel.xpath('//ksj:ED01').extract()
    count_coordinates = 0
    prefecture = prefecture_ascii = ''
    bulk = ""
    for place in places:
      sel_place = LocationBusSpider.get_selector_from_text(place)
      point_id = self.get_property(sel_place, '//pos/@idref')
      coordinates = self.get_property(sel, '//jps:GM_Point[@id="%s"]//DirectPosition.coordinate/text()' % point_id)
      coordinates = coordinates.split(' ')
      latitude = longitude = 0
      if coordinates and len(coordinates) >= 2:
        latitude = float(coordinates[0])
        longitude = float(coordinates[1])
        count_coordinates += 1

        if not prefecture:
          result = GeoLocation.reverse_geocode(latitude, longitude)
          if result:
            address = result[0]
            for areas in address['address_components']:
              if 'administrative_area_level_1' in areas['types']:
                prefecture = areas['long_name']
                for word in [u'県', u'府', u'都']:
                  if prefecture[-1] == word:
                    prefecture = prefecture.replace(word, u'')
                    break
                prefecture_ascii = LocationBusEs.analyze(prefecture, 'romaji_ascii_normal_analyzer')
                break

      item = LocationBusItem()
      item['name'] = self.get_property(sel_place, '//bsn/text()')
      item['latitude'] = latitude
      item['longitude'] = longitude
      item['prefecture'] = prefecture
      item['prefecture_ascii'] = prefecture_ascii

      data = LocationBusEs.get_data(item)
      if data:
        bulk += LocationBusEs.bulk_data(data)

      self.count += 1
      if (self.count % 100) == 0:
        LocationBusEs.send(bulk)
        bulk = ""
      print u'%s: %s -> (%s, %s)' % (self.count, item['name'], item['prefecture'], item['prefecture_ascii'])

    if bulk:
      LocationBusEs.send(bulk)
    print " "
    print "Results: "
    print "Coordinates: %s" % count_coordinates
    print "Total: %s" % self.count

  @classmethod
  def get_selector(cls, response):
    sel = Selector(response)
    for name, uri in cls.xml_namespaces:
      sel.register_namespace(name, uri)
    return sel

  @classmethod
  def get_selector_from_text(cls, text):
    text = u'<html><body>%s</body></html>' % text
    sel = Selector(text=text)
    for name, uri in cls.xml_namespaces:
      sel.register_namespace(name, uri)
    return sel

  def parse_err(self, failure):
    """save in the log the pages that couldn't be scrapped."""
    self.log_error(u'%s -- %s' % (failure.getErrorMessage(), failure.getBriefTraceback()))

  # def parse_airport(self, response):
  #   sel = Selector(response)
  #   item = LocationBusItem()
  #   item['name'] = self.get_property(sel, AirportSelectors.NAME)
  #   self.count += 1
  #   print u'%s: %s > %s -> %s' % (self.count, item['country'], item['area'], item['name'])
  #   return item
