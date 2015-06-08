# -*- coding: utf-8 -*-

from scrapy.selector import Selector
from scrapy.http import Request
from ghost_spider.items import SalonItem
from ghost_spider.helper import SalonSelectors
from ghost_spider.util import BaseSpider
from ghost_spider import helper
from ghost_spider.urls import URLS
from ghost_spider.elastic import SalonEs


class SalonSpider(BaseSpider):
  name = "salon"
  allowed_domains = ["localhost", "search.loco.yahoo.co.jp", "loco.yahoo.co.jp"]
  target_base_url = "http://search.loco.yahoo.co.jp"
  start_urls = (URLS['mie'] + URLS['okinawa'])
  # start_urls = ["file://localhost/Users/jctt/Developer/crawler/ghost_spider/samples/salons/list2.html"]
  count = 0

  def __init__(self, name=None, **kwargs):
    self.log_message('*-' * 50)
    self.log_message('Starting...')
    self.count = 0
    super(SalonSpider, self).__init__(self.name, **kwargs)

  def parse(self, response):
    sel = Selector(response)
    links = sel.xpath(SalonSelectors.LIST_SALONS).extract()
    next_page = self.get_property(sel, SalonSelectors.NEXT_URL)
    print u'links: %s, %s' % (len(links), response.url)
    if links:
      for link in links:
        canonical = link.split('?')[0]
        if SalonEs.check_by_url(canonical):
          print u'skipped: %s' % link
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
    item = SalonItem()
    item['page_url'] = self.get_property(sel, SalonSelectors.CANONICAL_URL) or response.url
    item['name'] = self.get_property(sel, SalonSelectors.NAME)
    item['name_kata'] = self.get_property(sel, SalonSelectors.NAME_KATA)
    item['address'] = self.get_property(sel, SalonSelectors.ADDRESS, clean=True)
    item['routes'] = SalonSelectors.get_routes(sel)
    item['phone'] = SalonSelectors.get_phone(sel)
    item['working_hours'] = SalonSelectors.get_working_hours(sel)
    item['holydays'] = SalonSelectors.get_holidays(sel)
    item['shop_url'] = SalonSelectors.get_shop_url(sel)

    comment, credit_cards = SalonSelectors.get_credit_cards(sel)
    item['credit_cards_comment'] = comment
    item['credit_cards'] = credit_cards

    item['seats'] = SalonSelectors.get_seats(sel)
    item['stylist'] = SalonSelectors.get_stylist(sel)
    item['parking'] = SalonSelectors.get_parking(sel)

    item['cut_price'] = SalonSelectors.get_cut_price(sel)
    prefecture, area = SalonSelectors.get_prefecture_area(sel)

    item['prefecture'] = prefecture
    item['area'] = area

    item['page_body'] = SalonSelectors.get_body(sel)
    self.count += 1
    print u'%s: %s > %s -> %s' % (self.count, item['prefecture'], item['area'], item['name'])
    return item
