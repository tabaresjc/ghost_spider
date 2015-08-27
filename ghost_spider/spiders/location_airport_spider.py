# -*- coding: utf-8 -*-

from scrapy.selector import Selector
from scrapy.http import Request
from ghost_spider.items import LocationAirportItem
from ghost_spider.helper import AirportSelectors
from ghost_spider.data import URLS, GOURMET_CATEGORY
from ghost_spider.elastic import LocationAirportEs
from ghost_spider.util import BaseSpider
from ghost_spider import helper


class LocationAirportSpider(BaseSpider):
  name = "location_airport"
  allowed_domains = ["localhost", "flyteam.jp"]
  base_url = 'http://flyteam.jp'
  start_urls = [
    "http://flyteam.jp/airports/iata/A",
    # "http://flyteam.jp/airports/iata/B",
    # "http://flyteam.jp/airports/iata/C",
    # "http://flyteam.jp/airports/iata/D",
    # "http://flyteam.jp/airports/iata/E",
    # "http://flyteam.jp/airports/iata/F",
    # "http://flyteam.jp/airports/iata/G",
    # "http://flyteam.jp/airports/iata/H",
    # "http://flyteam.jp/airports/iata/I",
    # "http://flyteam.jp/airports/iata/J",
    # "http://flyteam.jp/airports/iata/K",
    # "http://flyteam.jp/airports/iata/L",
    # "http://flyteam.jp/airports/iata/M",
    # "http://flyteam.jp/airports/iata/N",
    # "http://flyteam.jp/airports/iata/O",
    # "http://flyteam.jp/airports/iata/P",
    # "http://flyteam.jp/airports/iata/Q",
    # "http://flyteam.jp/airports/iata/R",
    # "http://flyteam.jp/airports/iata/S",
    # "http://flyteam.jp/airports/iata/T",
    # "http://flyteam.jp/airports/iata/U",
    # "http://flyteam.jp/airports/iata/V",
    # "http://flyteam.jp/airports/iata/W",
    # "http://flyteam.jp/airports/iata/X",
    # "http://flyteam.jp/airports/iata/Y",
    # "http://flyteam.jp/airports/iata/Z",
  ]
  count = 0
  total = 0
  scan_mode = False

  def __init__(self, name=None, **kwargs):
    self.log_message('*-' * 50)
    self.log_message('Starting...')
    self.count = 0
    self.total = 0
    super(LocationAirportSpider, self).__init__(self.name, **kwargs)

  def parse(self, response):
    sel = Selector(response)
    links = sel.xpath(AirportSelectors.LIST_AIRPORTS).extract()
    current_page = long(response.meta.get('current_page') or 1)
    print u'links: %s, %s' % (len(links), response.url)

    if links:
      for link in links:
        canonical = u'%s%s' % (self.base_url, link.split('?')[0])
        if LocationAirportEs.check_by_url(canonical):
          continue
        request = Request(canonical, callback=self.parse_airport, errback=self.parse_err)
        request.meta['page_kind'] = 'airport'
        yield request
      if len(links) >= 100:
        next_page = u'%s?pageid=%s' % (response.url.split('?')[0], current_page + 1)
        request = Request(next_page, callback=self.parse, errback=self.parse_err)
        request.meta['page_kind'] = 'list'
        request.meta['current_page'] = current_page + 1
        yield request

  def parse_err(self, failure):
    """save in the log the pages that couldn't be scrapped."""
    self.log_error(u'%s -- %s' % (failure.getErrorMessage(), failure.getBriefTraceback()))

  def parse_airport(self, response):
    sel = Selector(response)
    item = LocationAirportItem()
    item['page_url'] = response.url.split('?')[0]
    item['name'] = self.get_property(sel, AirportSelectors.NAME)
    item['name_eng'] = self.get_property(sel, AirportSelectors.NAME_ENG)
    area, country = AirportSelectors.get_area_info(sel)
    code, code2 = AirportSelectors.get_airport_info(sel)
    item['area'] = area
    item['country'] = country
    item['code'] = code
    item['code2'] = code2
    item['breadcrumbs'] = ''
    return item
