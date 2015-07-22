# -*- coding: utf-8 -*-

from scrapy.selector import Selector
from scrapy.http import Request
from ghost_spider.items import LocationRestaurantItem
from ghost_spider.helper import LocationHotelSelectors
from ghost_spider.data import URLS, GOURMET_CATEGORY
from ghost_spider.elastic import LocationRestaurantEs
from ghost_spider.util import BaseSpider
from ghost_spider import helper


class LocationHotelSpider(BaseSpider):
  name = "location_restaurant"
  allowed_domains = ["localhost", "search.loco.yahoo.co.jp", "loco.yahoo.co.jp"]
  target_base_url = "http://search.loco.yahoo.co.jp"
  start_urls = URLS['tokyo']
  count = 0
  total = 0
  scan_mode = False

  def __init__(self, name=None, **kwargs):
    self.log_message('*-' * 50)
    self.log_message('Starting...')
    self.count = 0
    self.total = 0
    super(LocationHotelSpider, self).__init__(self.name, **kwargs)

  def parse(self, response):
    sel = Selector(response)
    links = sel.xpath(LocationHotelSelectors.LIST_SALONS).extract()
    next_page = self.get_property(sel, LocationHotelSelectors.NEXT_URL)
    print u'links: %s, %s' % (len(links), response.url)

    if len(links) <= 0:
      self.log_message(u'links: %s, %s' % (len(links), response.url))

    if LocationHotelSelectors.is_first_page(sel):
      total = LocationHotelSelectors.get_list_total(sel)
      self.total += total
      if total > 999:
        # yahoo search can not paginate beyond 1000 items
        # so need to run crawler for smaller areas or cateories
        page_cat = LocationHotelSelectors.get_category(sel)
        if page_cat and page_cat != "01":
          self.log_message(u'Pagination overflow: %s' % response.url)
        else:
          for category in GOURMET_CATEGORY:
            next_page = response.url.replace('genrecd=01', 'genrecd=%s' % category)
            print u'new links --> %s' % next_page
            request = Request(next_page, callback=self.parse, errback=self.parse_err)
            request.meta['page_kind'] = 'list'
            yield request

      if self.start_urls[-1] == response.url:
        self.log_message(u'Counted this many places: %s' % self.total)

    if self.scan_mode:
      return

    if links:
      for link in links:
        canonical = link.split('?')[0]
        if LocationRestaurantEs.check_by_url(canonical):
          # print u'skipped: %s' % link
          continue
        request = Request(link, callback=self.parse_salon, errback=self.parse_err)
        request.meta['page_kind'] = 'salon'
        yield request

    if next_page:
      request = Request(next_page, callback=self.parse, errback=self.parse_err)
      request.meta['page_kind'] = 'list'
      yield request

  def parse_err(self, failure):
    """save in the log the pages that couldn't be scrapped."""
    self.log_error(u'%s -- %s' % (failure.getErrorMessage(), failure.getBriefTraceback()))

  def parse_salon(self, response):
    sel = Selector(response)
    item = LocationRestaurantItem()
    item['page_url'] = self.get_property(sel, LocationHotelSelectors.CANONICAL_URL) or response.url
    item['name'] = self.get_property(sel, LocationHotelSelectors.NAME)
    item['name_kata'] = self.get_property(sel, LocationHotelSelectors.NAME_KATA)
    item['address'] = self.get_property(sel, LocationHotelSelectors.ADDRESS, clean=True)
    item['phone'] = LocationHotelSelectors.get_phone(sel)

    prefecture, area = LocationHotelSelectors.get_prefecture_area(sel)

    item['prefecture'] = prefecture
    item['area'] = area
    genre = LocationHotelSelectors.get_restaurant_genre(sel)
    item['genre'] = genre
    item['kind'] = LocationHotelSelectors.convert_latte_kind(genre)

    item['page_body'] = LocationHotelSelectors.get_body(sel, is_restaurant=True)
    self.count += 1
    print u'%s: %s > %s -> %s' % (self.count, item['prefecture'], item['area'], item['name'])

    return item
