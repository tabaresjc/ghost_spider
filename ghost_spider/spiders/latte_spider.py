# -*- coding: utf-8 -*-

from scrapy.selector import Selector
from scrapy.http import Request
from ghost_spider.items import SalonItem
from ghost_spider.helper import SalonSelectors
from ghost_spider.util import BaseSpider
from ghost_spider import helper
from ghost_spider.urls import URLS
from ghost_spider.elastic import SalonEs


class LatteSpider(BaseSpider):
  name = "latte_spider"
  allowed_domains = ["localhost", "latte.la"]
  start_urls = URLS['latte']
  handle_httpstatus_list = [400, 404, 500]
  # start_urls = ["file://localhost/Users/jctt/Developer/crawler/ghost_spider/samples/salons/list2.html"]

  def __init__(self, name=None, **kwargs):
    self.log_message('*-' * 50)
    self.log_message('Starting...')
    self.count = 0
    self.count_skip = 0
    super(LatteSpider, self).__init__(self.name, **kwargs)

  def parse(self, response):
    if response.status in [400, 404, 500]:
      message = u'%s -- %s' % (response.status, response.url)
      self.log_error(message)
      print u'ERROR --  %s' % message

  def parse_err(self, failure):
    """save in the log the pages that couldn't be scrapped."""
    self.log_error(u'%s -- %s' % (failure.getErrorMessage(), failure.getBriefTraceback()))
