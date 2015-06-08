# -*- coding: utf-8 -*-

from scrapy.selector import Selector
from scrapy.http import Request
from ghost_spider import helper
from ghost_spider.items import HotelItem
from ghost_spider.elastic import LocationEs
from ghost_spider.util import BaseSpider


class HotelSpider(BaseSpider):
  name = "hotel"
  allowed_domains = ["localhost", "tripadvisor.com", "tripadvisor.jp", "tripadvisor.es", "tripadvisor.fr", "daodao.com"]
  target_base_url = "http://www.tripadvisor.com"
  start_urls = ["http://localhost/AllLocations-g1-c1-Hotels-World.html"]
  total_count = 0L

  def __init__(self, name=None, **kwargs):
    self.total_count = 0L
    super(HotelSpider, self).__init__(self.name, **kwargs)

  def parse(self, response):
    """Go through the sitemap and fetch hotels/restaurant/spot pages."""
    count = 0
    download_list = None
    current_level = long(response.meta.get('area_level') or 1)
    sel = Selector(response)
    links = sel.xpath(helper.SEL_LIST_PLACES).extract()

    # Get the list of countries that needs to be scrapped
    if current_level == 1:
      download_list = sel.xpath(helper.SEL_ALLOW_PLACES).extract()
      if download_list:
        download_list = download_list[0].split(u',')
    if links:
      for link in links:
        area_name = helper.place_sel_name.findall(link)[0]
        # skip country if is not in the list
        if download_list and area_name.lower() not in download_list:
          continue
        area_link = self.target_base_url + helper.place_sel_link.findall(link)[0]
        count += 1
        request = Request(area_link, callback=self.parse, errback=self.parse_err)
        request.meta['area_name'] = area_name
        request.meta['area_level'] = current_level + 1
        yield request
    else:
      # possible last level
      links = sel.xpath(helper.SEL_LIST_PLACES_LAST).extract()
      if links:
        if not response.meta.get('is_more'):
          # load additional list of places
          links_more = sel.xpath(helper.SEL_LIST_MORE).extract()
          for l in links_more:
            count += 1
            area_name = "More Links"
            area_link = self.target_base_url + helper.place_sel_link.findall(l)[0]
            request = Request(area_link, callback=self.parse, errback=self.parse_err)
            request.meta['area_name'] = area_name
            request.meta['is_more'] = True
            request.meta['area_level'] = current_level
            self.log.msg('Loading more pages, %s' % area_link, level=self.log.INFO)
            yield request
        for link in links:
          area_name = helper.place_sel_name_last.findall(link)[0]
          area_link = self.target_base_url + helper.place_sel_link_last.findall(link)[0]
          # don't scrap the page if it was crawled
          # if the link is not hotel don't fetch it!!
          if not helper.FIND_HOTEL_LINK.findall(area_link):
            self.log.msg(u'ignored %s' % area_link, level=self.log.INFO)
            continue
          if LocationEs.check_by_url(area_link):
            self.log.msg(u'ignored %s' % area_link, level=self.log.INFO)
            continue
          request = Request(area_link, callback=self.parse_place, errback=self.parse_err)
          request.meta['area_name'] = area_name
          request.meta['area_level'] = current_level + 1
          yield request
          count += 1
        self.total_count += count
        print u'found = %s' % self.total_count
    if response.meta.get('area_name'):
      message = u'%s> %s found(%s) | total(%s)' % ('-----' * current_level, response.meta['area_name'], count, self.total_count)
      print message
      self.log.msg(message, level=self.log.INFO)

  def parse_err(self, failure):
    """save in the log failure."""
    # save in the log the pages that couldn't be scrapped
    self.log.error(u'%s -- %s' % (failure.getErrorMessage(), failure.getBriefTraceback()))

  def parse_place(self, response):
    """Parse hotel/restaurant/spot page."""
    if response.meta.get('area_name') and self.log:
      self.log.msg(u'%s> %s' % ("-----" * response.meta.get('area_level') or 1, response.meta['area_name']), level=self.log.INFO)
    sel = Selector(response)
    item = HotelItem()

    item['page_url'] = response.url
    item['page_breadcrumbs'] = sel.xpath(helper.SEL_BREADCRUMBS).extract()
    item['name'] = sel.xpath(helper.SEL_HOTEL_NAME).extract()
    item['phone'] = sel.xpath(helper.SEL_PHONE_NUMBER).extract()
    item['rating'] = sel.xpath(helper.SEL_RATING).re(r'(.*)\s*of 5')
    item['popularity'] = sel.xpath(helper.SEL_PERCENT).re(r'(.*)\s*%')
    item['region'] = sel.xpath(helper.SEL_AREA_REGION).extract()
    place = {
      'lang': 'en',
      'name': item['name'],
      'address_area_name': sel.xpath(helper.SEL_AREA_NAME).extract(),
      'address_street': sel.xpath(helper.SEL_AREA_STREET).extract(),
      'address_locality': sel.xpath(helper.SEL_AREA_LOCALITY).extract(),
      'address_region': sel.xpath(helper.SEL_AREA_REGION).extract(),
      'address_zip': sel.xpath(helper.SEL_AREA_ZIP).extract(),
      'amenity': sel.xpath(helper.SEL_AMENITIES).extract(),
      'page_body': helper.get_body(sel)
    }
    # save list of places by language
    item['place'] = [place]

    links = {
      'ja': sel.xpath(helper.SEL_JAPANESE_PAGE).extract(),
    }
    remain = ['ja']
    if self.need_french_page(item['page_breadcrumbs']):
      links['fr'] = sel.xpath(helper.SEL_FRENCH_PAGE).extract()
      remain.append('fr')
    elif self.need_spanish_page(item['page_breadcrumbs']):
      links['es'] = sel.xpath(helper.SEL_SPANISH_PAGE).extract()
      remain.append('es')

    for name, link in links.iteritems():
      if not link:
        self.log.error("couldn't index this page | %s" % response.url)
        return None
      links[name] = link[0]
    request = Request(links['ja'], callback=self.parse_local_page)
    request.meta['remain'] = remain
    request.meta['links'] = links
    request.meta['item'] = item
    return request

  def parse_local_page(self, response):
    """Parse hotel/restaurant/spot page in different language."""
    current_lang = response.meta['remain'][0]
    remain = response.meta['remain'][1:]
    sel = Selector(response)
    item = response.meta['item']
    place = {
      'lang': current_lang,
      'name': sel.xpath(helper.SEL_HOTEL_NAME).extract(),
      'address_area_name': sel.xpath(helper.SEL_AREA_NAME).extract(),
      'address_street': sel.xpath(helper.SEL_AREA_STREET).extract(),
      'address_locality': sel.xpath(helper.SEL_AREA_LOCALITY).extract(),
      'address_region': sel.xpath(helper.SEL_AREA_REGION).extract(),
      'address_zip': sel.xpath(helper.SEL_AREA_ZIP).extract(),
      'amenity': sel.xpath(helper.SEL_AMENITIES).extract(),
      'page_body': helper.get_body(sel)
    }
    item['place'].append(place)
    if remain and len(remain) > 0:
      from ghost_spider.settings import REQUEST_HEADERS
      next_lang = remain[0]
      request = Request(response.meta['links'][next_lang], headers=REQUEST_HEADERS[next_lang], callback=self.parse_local_page, errback=self.parse_err)
      request.meta['remain'] = remain
      request.meta['links'] = response.meta['links']
      request.meta['item'] = item
      return request
    return item

  def need_french_page(self, breadcrumbs):
    countries = [u'France', u'St Maarten-St Martin', u'Guadeloupe', u'French Guiana', u'Wallis and Futuna',
    u'St. Barthelemy', u'Martinique', 'Saint-Pierre and Miquelon']
    return breadcrumbs[0] in countries or breadcrumbs[1] in countries or breadcrumbs[2] in countries

  def need_spanish_page(self, breadcrumbs):
    countries = [u'Spain', u'Mexico', u'Argentina', u'Chile', u'Ecuador', u'Peru', u'Venezuela', u'Costa Rica', u'Guatemala',
    u'Honduras', u'El Salvador', u'Nicaragua', u'Panama', u'Uruguay', u'Bolivia', u'Colombia', u'Paraguay']
    return breadcrumbs[0] in countries or breadcrumbs[1] in countries or breadcrumbs[2] in countries
