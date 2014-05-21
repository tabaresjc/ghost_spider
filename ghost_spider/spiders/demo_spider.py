from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from ghost_spider.items import GhostSpiderItem
from ghost_spider import helper
from ghost_spider.helper import debug_screen, clean_lf, rev_telephone


class DemoSpider(Spider):
  name = "demo"
  allowed_domains = ["localhost"]
  start_urls = [
      "file://localhost/Users/jctt/Developer/crawler/ghost_spider/samples/target_hotel_page.html",
  ]

  def parse(self, response):
    sel = Selector(response)
    item = GhostSpiderItem()
    item['name'] = sel.xpath(helper.SEL_HOTEL_NAME).extract()
    item['phone'] = sel.xpath(helper.SEL_PHONE_NUMBER).extract()
    item['address_area_name'] = sel.xpath(helper.SEL_AREA_NAME).extract()
    item['address_street'] = sel.xpath(helper.SEL_AREA_STREET).extract()
    item['address_locality'] = sel.xpath(helper.SEL_AREA_LOCALITY).extract()
    item['address_region'] = sel.xpath(helper.SEL_AREA_REGION).extract()
    item['address_zip'] = sel.xpath(helper.SEL_AREA_ZIP).extract()
    item['amenity'] = sel.xpath(helper.SEL_AMENITIES).extract()
    
    links = {
      'es': sel.xpath(helper.SEL_SPANISH_PAGE).extract(),
      'ja': sel.xpath(helper.SEL_JAPANESE_PAGE).extract(),
      'za': sel.xpath(helper.SEL_CHINESE_PAGE).extract()
    }
    # load the same page in different language
    for name, link in links.iteritems():
      links[name] = link[0]

    request = Request(links['ja'], callback=self.parse_local_page)
    request.meta['remain'] = ['ja', 'es', 'za']
    request.meta['links'] = links
    request.meta['item'] = item
    return request

  def parse_local_page(self, response):
    current = response.meta['remain'][0]
    remain = response.meta['remain'][1:]
    sel = Selector(response)
    item = response.meta['item']
    item['name_%s' % current] = sel.xpath(helper.SEL_HOTEL_NAME).extract()
    item['address_area_name_%s' % current] = sel.xpath(helper.SEL_AREA_NAME).extract()
    item['address_street_%s' % current] = sel.xpath(helper.SEL_AREA_STREET).extract()
    item['address_locality_%s' % current] = sel.xpath(helper.SEL_AREA_LOCALITY).extract()
    item['address_region_%s' % current] = sel.xpath(helper.SEL_AREA_REGION).extract()
    item['address_zip_%s' % current] = sel.xpath(helper.SEL_AREA_ZIP).extract()
    if remain and len(remain) > 0:
      request = Request(response.meta['links'][remain[0]], callback=self.parse_local_page)
      request.meta['remain'] = remain
      request.meta['links'] = response.meta['links']
      request.meta['item'] = item
      return request

    # clean the values
    for k, v in item.iteritems():
      if k == 'phone':
        item[k] = rev_telephone(v[0] if len(v) else u'')
      elif k == 'amenity':
        item[k] = clean_lf(v, u', ')
      else:
        item[k] = clean_lf(v)
    return item
