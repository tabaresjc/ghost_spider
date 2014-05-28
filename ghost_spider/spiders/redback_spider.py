# -*- coding: utf-8 -*-

from scrapy.spider import Spider
from ghost_spider import helper
import shutil
import os
import logging
from scrapy import log as scrapyLog
from ghost_spider.elastic import PlaceHs
from ghost_spider.util import CsvWriter


class RedBackSpider(Spider):
  name = "redback"
  allowed_domains = ["localhost"]
  target_base_url = "file://localhost/Users/jctt/Developer/crawler/ghost_spider/samples"
  start_urls = [
      "file://localhost/Users/jctt/Developer/crawler/ghost_spider/samples/target_list_of_places.html"
  ]
  log = None
  total_count = 0L
  LOG_OUTPUT_FILE_INFO = "/Users/jctt/Developer/crawler/output/info-log.txt"
  ES_SORT = [{"place.address_region": "asc"}, {"popularity": "desc"}]
  first_row = [
    u'name',
    u'name_ja',
    u'name_es',
    u'name_fr',
    u'name_zh',
    u'address',
    u'address_ja',
    u'address_es',
    u'address_fr',
    u'address_zh',
    u'phone',
    u'amenity',
    u'amenity_ja',
    u'amenity_es',
    u'amenity_fr',
    u'amenity_zh',
    u'popularity',
    u'id'
  ]

  def __init__(self, name=None, **kwargs):
    self.log = logging.getLogger(self.name)
    ch = logging.FileHandler(self.LOG_OUTPUT_FILE_INFO)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    ch.setLevel(logging.ERROR)
    ch.setFormatter(formatter)
    self.log.addHandler(ch)
    self.total_count = 0L
    super(RedBackSpider, self).__init__(self.name, **kwargs)

  def parse(self, response):
    page = 1
    limit = 10
    shutil.rmtree('upload/')
    while True:
      places, total = PlaceHs.pager(page=page, size=limit, sort=self.ES_SORT)
      page += 1
      if not places or not len(places):
        break
      for p in places:
        print u'Saving %s > %s > %s' % (p.get('area1'), p.get('area2'), p['place'][0]['name'])
        self.save_to_csv(p)
      if page > 4:
        break

  def save_to_csv(self, item):
    filename = self.get_filename(item, u'hotels.csv')
    row = []

    for p in item['place']:
      for k, v in p.iteritems():
        item[k] = v

    row.append(item['name'])
    row.append(item['name_ja'])
    row.append(item['name_es'])
    row.append(item['name_fr'])
    row.append(item.get('name_zh') or u'')
    row.append(u'%s, %s, %s %s%s' % (item['address_street'], item['address_locality'], item['address_region'], item['address_zip'], item['address_area_name']))
    row.append(u'%s, %s, %s %s%s' % (item['address_street_ja'], item['address_locality_ja'], item['address_region_ja'], item['address_zip_ja'], item['address_area_name_ja']))
    row.append(u'%s, %s, %s %s%s' % (item['address_street_es'], item['address_locality_es'], item['address_region_es'], item['address_zip_es'], item['address_area_name_es']))
    row.append(u'%s, %s, %s %s%s' % (item['address_street_fr'], item['address_locality_fr'], item['address_region_fr'], item['address_zip_fr'], item['address_area_name_fr']))
    if item.get('address_street_zh'):
      row.append(u'%s, %s, %s %s%s' % (item['address_street_zh'], item['address_locality_zh'], item['address_region_zh'], item['address_zip_zh'], item['address_area_name_zh']))
    else:
      row.append(u'')
    row.append(item['phone'])
    row.append(item['amenity'])
    row.append(item['amenity_ja'])
    row.append(item['amenity_es'])
    row.append(item['amenity_fr'])
    row.append(item.get('amenity_zh') or u'')
    row.append(u'%s%%' % item['popularity'])
    row.append(item['id'])
    CsvWriter.write_to_csv(filename, row, firs_row=self.first_row)

  def get_filename(self, item, filename):
    if not item.get('area1') or not item.get('area2'):
      return None
    directory = u'upload/%s/%s' % (item.get('area1'), item.get('area2'))
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = u'%s/%s' % (directory, filename)
    return filename
