# -*- coding: utf-8 -*-

from scrapy.spider import Spider
from ghost_spider.settings import USER_AGENT_LIST, USER_AGENT
from ghost_spider.helper import LocationHotelSelectors
from random import randint
import logging
import os.path
import csv, codecs, cStringIO
import shutil
import os
import zenhan
import re


class BaseSpider(Spider):
  name = "ghost_spider"
  allowed_domains = ["localhost"]
  target_base_url = ""
  start_urls = []
  mode_scan = False

  @property
  def log(self):
    """return log handler."""
    try:
      return self._log
    except:
      from scrapy import log
      self._log = log
      return self._log

  @property
  def custom_log(self):
    """return custom log handler."""
    try:
      return self._custom_log
    except:
      from ghost_spider.settings import LOG_OUTPUT_DIR
      logger = logging.getLogger(self.name)
      logger.setLevel(logging.DEBUG)
      # create console handler and set level to debug
      ch = logging.FileHandler(LOG_OUTPUT_DIR + "spider-log-%s.txt" % self.name)
      ch.setLevel(logging.DEBUG)
      # create formatter
      formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      ch.setFormatter(formatter)
      logger.addHandler(ch)
      self._custom_log = logger
      return self._custom_log

  def log_message(self, message):
    """return custom log handler."""
    self.custom_log.info(message)

  def log_error(self, error_message):
    """return custom log handler."""
    self.custom_log.error(error_message)

  @property
  def user_agent(self):
    try:
      nlen = len(USER_AGENT_LIST)
      n = randint(0, nlen - 1)
      return USER_AGENT_LIST[n]
    except:
      return USER_AGENT

  def get_property(self, sel, selector, clean=False):
    pro = u''
    try:
      pro = sel.xpath(selector).extract()
      if clean:
        # when data is not in the first element of the array
        pro = u''.join(pro).strip()
      else:
        # use this when you are certain that neccesary info is always on
        # the first element of the array
        pro = pro[0] if pro and len(pro) else u''
        pro = pro.strip()
    except:
      pro = u''
    return pro


class CsvWriter:
  """
  A CSV writer which will write rows to CSV file "f",
  which is encoded in the given encoding.
  """

  def __init__(self, f, dialect=csv.excel, encoding="utf-8", region="US", **kwds):
    self.queue = cStringIO.StringIO()
    self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
    self.stream = f
    self.encoder = codecs.getincrementalencoder(encoding)()

  @classmethod
  def write_to_csv(cls, filename, rows, firs_row=[]):
    is_blank = not os.path.isfile(filename)
    csvw = cls(open(filename, 'a+'), dialect='excel')
    if is_blank and firs_row:
      csvw.writerow(firs_row)
    csvw.writerow(rows)

  def writerow(self, row):
    self.writer.writerow([s.encode("utf-8") for s in row])
    # Fetch UTF-8 output from the queue ...
    data = self.queue.getvalue()
    data = data.decode("utf-8")
    # ... and reencode it into the target encoding
    data = self.encoder.encode(data)
    # write to the target stream
    self.stream.write(data)
    # empty queue
    self.queue.truncate(0)

  def writerows(self, rows):
    for row in rows:
      self.writerow(row)


class CsvImporter(object):

  """Class with specific stuff for csv import/export."""

  # list of fields name to build header for export or read data in import
  fieldnames = ('id', 'name', 'kana', 'address', 'phone', 'url_key', 'url')

  @classmethod
  def import_file(cls, filename, kind):
    """Store data from csv files.

    filename: string
    kind: hotel/restaurant

    """
    import csv
    import progressbar
    import time
    from ghost_spider.elastic import LatteHotelEs, LatteRestaurantEs

    to_class = None
    if kind == 'hotel':
      to_class = LatteHotelEs
    elif kind == 'restaurant':
      to_class = LatteRestaurantEs
    else:
      raise NotImplementedError()

    csvfile = open(filename, 'rb')
    fieldnames = cls.fieldnames
    reader = csv.DictReader(csvfile, fieldnames)

    try:
      to_class.DEBUG = False
      next(reader)  # skip the title line
      rows = list(reader)
      total = len(rows)
      progress = progressbar.AnimatedProgressBar(end=total, width=100)
      bulk = ""
      count_lines = 0
      for line, row in enumerate(rows):
        progress += 1
        progress.show_progress()
        data = {}
        for k, v in row.iteritems():
          if v:
            if not isinstance(v, (list, tuple)):
              data.update({k: v.decode('utf-8')})
        data["name_low"] = data["name"].lower()
        data["name_cleaned"] = to_class.analyze(data["name"].lower(), 'baseform_analyzer')
        data["name_cleaned"] = zenhan.z2h(data["name_cleaned"], zenhan.ASCII)
        data["url"] = data["url"].lower()
        data["kind"] = data["kind"].split('|') if data.get('kind') else []
        bulk += to_class.bulk_data(data, action="create")
        count_lines += 1
        if (count_lines % 200) == 0:
          to_class.send(bulk)
          bulk = ""

      if bulk:
        to_class.send(bulk)
      progress.show_progress()
      print " "
    finally:
      if csvfile:
        csvfile.close()


class LocationHotelsToCsvFiles(object):
  US_REGION = 0
  UE_REGION = 1
  region = US_REGION
  name = "ghost_spider"
  LOG_OUTPUT_FILE_INFO = "upload/info-log.txt"
  ES_SORT = [
    {"region": "asc"},
    {"popularity": "desc"}
  ]
  ES_SORT_STATE = [
    {"place.area2.untouched": "asc"},
    {"popularity": "desc"}
  ]
  TARGET_DIR_FORMAT = u'upload/crawler/hotels/%s/%s'
  TARGET_DIR_FORMAT_STATE = u'upload/crawler/hotels/%s/%s/%s'
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

  def __init__(self, region, **kwargs):
    """Class initializer.

    region: string

    """
    if region == "US":
      self.region = self.US_REGION
    elif region == "EU":
      self.region = self.UE_REGION
    super(LocationHotelsToCsvFiles, self).__init__(**kwargs)

  @property
  def logger(self):
    """error log handler."""
    try:
      return self._logger
    except AttributeError:
      if not os.path.exists('upload'):
          os.makedirs('upload')
      self._logger = logging.getLogger(self.name)
      ch = logging.FileHandler(self.LOG_OUTPUT_FILE_INFO)
      formatter = logging.Formatter('%(asctime)s - %(message)s')
      ch.setFormatter(formatter)
      ch.setLevel(logging.ERROR)
      self._logger.addHandler(ch)
      self.total_count = 0L
    return self._logger

  def dump(self):
    from ghost_spider.elastic import LocationEs
    from ghost_spider import progressbar
    page = 1
    limit = 100

    parent_place = "United States"
    sort = self.ES_SORT
    directory = 'upload/crawler/hotels/United States'
    if self.region == self.UE_REGION:
      parent_place = "Europe"
      sort = self.ES_SORT_STATE
      directory = 'upload/crawler/hotels/Europe'
    query = {"query": {"bool": {"must": [{"term": {"place.area1.untouched": parent_place}}]}}}

    if os.path.exists(directory):
      shutil.rmtree(directory)

    progress = None
    total = 0
    while True:
      places, total = LocationEs.pager(query=query, page=page, size=limit, sort=sort)
      page += 1
      if not places or not len(places):
        break
      if not progress:
        progress = progressbar.AnimatedProgressBar(end=total, width=100)
      progress + limit
      progress.show_progress()
      for p in places:
        self.save_to_csv(p)
    print " "
    print "Finito!"
    print "*." * 50
    print "count %s" % total

  def save_to_csv(self, item):
    filename = self.get_filename(item, u'hotels.csv')
    row = []

    for p in item['place']:
      item['name_%s' % p['lang']] = p.get('name')
      item['amenity_%s' % p['lang']] = p.get('amenity')
      item['address_%s' % p['lang']] = self.build_address(p)

    row.append(item.get('name_en') or u'')
    row.append(item.get('name_ja') or u'')
    row.append(item.get('name_es') or u'')
    row.append(item.get('name_fr') or u'')
    row.append(item.get('name_zh') or u'')

    row.append(item.get('address_en') or u'')
    row.append(item.get('address_ja') or u'')
    row.append(item.get('address_es') or u'')
    row.append(item.get('address_fr') or u'')
    row.append(item.get('address_zh') or u'')
    row.append(item['phone'])
    row.append(item.get('amenity_en') or u'')
    row.append(item.get('amenity_ja') or u'')
    row.append(item.get('amenity_es') or u'')
    row.append(item.get('amenity_fr') or u'')
    row.append(item.get('amenity_zh') or u'')
    row.append(u'%s%%' % item['popularity'])
    row.append(item['id'])
    CsvWriter.write_to_csv(filename, row, firs_row=self.first_row)

  def get_filename(self, item, filename):
    if not item.get('area1') or not item.get('area2'):
      return None
    directory = self.TARGET_DIR_FORMAT % (item.get('area1').strip(), item.get('area2').strip())
    if self.region == self.UE_REGION:
      directory = self.TARGET_DIR_FORMAT_STATE % (item.get('area1').strip(), item.get('area2').strip(), item.get('area3').strip())
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = u'%s/%s' % (directory, filename)
    return filename

  def build_address(self, place):
    address_list = []
    if place.get('address_street'):
      address_list.append(place['address_street'])
    if place.get('address_locality'):
      address_list.append(place['address_locality'])
    if place.get('address_locality'):
      address_list.append(place['address_locality'])
    if place.get('address_region'):
      address_list.append(place['address_region'])
    if place.get('address_zip'):
      address_list.append(place['address_zip'])
    if place.get('address_area_name'):
      address_list.append(place['address_area_name'])
    return u', '.join(address_list)


class SalonToCsvFiles(object):
  name = None
  LOG_OUTPUT_FILE_INFO = "upload/info-salon-log.txt"
  TARGET_DIR_FORMAT = u'output/salons/%s'
  first_row = [
    u'サロン名',
    u'サロン名(カナ)',
    u'住所',
    u'最寄駅',
    u'都道府県',
    u'エリア',
    u'電話番号',
    u'営業時間',
    u'定休日',
    u'ホームページ',
    u'クレジットカード',
    u'クレジットカード(KIND)',
    u'座席数',
    u'スタイリスト数',
    u'駐車場',
    u'カット価格',
    u'URL',
  ]

  def __init__(self, name, **kwargs):
    """Class initializer.

    name: string

    """
    self.name = name
    super(SalonToCsvFiles, self).__init__(**kwargs)

  @property
  def logger(self):
    """error log handler."""
    try:
      return self._logger
    except AttributeError:
      if not os.path.exists('upload'):
          os.makedirs('upload')
      self._logger = logging.getLogger(self.name)
      ch = logging.FileHandler(self.LOG_OUTPUT_FILE_INFO)
      formatter = logging.Formatter('%(asctime)s - %(message)s')
      ch.setFormatter(formatter)
      ch.setLevel(logging.ERROR)
      self._logger.addHandler(ch)
      self.total_count = 0L
    return self._logger

  def dump(self, action=None):
    from ghost_spider.elastic import SalonEs
    from ghost_spider import progressbar

    filename = self.get_filename_by_name(self.name, u'salons.csv')
    query = {"query": {"bool": {"must": [{"term": {"prefecture_ascii": self.name}}]}}}

    if action == 'recover':
      query["query"]["bool"]["must"].append({"term": {"recovered": "1"}})
      filename = self.get_filename_by_name(self.name, u'salons_recover.csv')

    if os.path.exists(filename):
      os.remove(filename)

    progress = None
    total = 0
    page = 1
    limit = 100
    sort = [{"area.untouched": "asc"}]
    print "=" * 100
    print "dumping data for %s" % self.name
    while True:
      salons, total = SalonEs.pager(query=query, page=page, size=limit, sort=sort)
      page += 1
      if not salons or not len(salons):
        break
      if not progress:
        print u'Total: %s' % total
        progress = progressbar.AnimatedProgressBar(end=total, width=100)
      progress + limit
      progress.show_progress()
      for salon in salons:
        self.save_to_csv(filename, salon)
    print " "

  def save_to_csv(self, filename, data):
    """Save this data on csv file by prefecture"""
    row = []
    address = zenhan.z2h(data['address'], zenhan.ALL)
    # remove the zip code
    address = re.sub(r'%s\d+-\d+' % u'〒', '', address).strip()

    row.append(data['name'])
    row.append(data['name_kata'])
    row.append(address)
    row.append(u'\n'.join(data['routes'] or u''))

    row.append(data['prefecture'])
    row.append(data['area'])

    row.append(zenhan.z2h(data['phone'], zenhan.ALL))
    row.append(data['working_hours'])
    row.append(data['holydays'])
    row.append(data['shop_url'])

    row.append(data['credit_cards_comment'])
    row.append(u'・'.join(data['credit_cards'] or u''))

    row.append(data['seats'])
    row.append(data['stylist'])
    row.append(data['parking'])
    row.append(unicode(data['cut_price']))
    row.append(data['page_url'])

    CsvWriter.write_to_csv(filename, row, firs_row=self.first_row)

  def get_filename_by_name(self, name, filename):
    directory = self.TARGET_DIR_FORMAT % name
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = u'%s/%s' % (directory, filename)
    return filename


class LocationCsv(object):
  name = None
  LOG_OUTPUT_FILE_INFO = "upload/info-salon-log.txt"
  TARGET_DIR_FORMAT = u'output/restaurants'

  first_row = [u'name', u'kana', u'address', u'prefecture', u'area', u'phone', u'kind', u'travel_url', u'url', u'_id']
  production_first_row = [u'name', u'kana', u'address', u'parent_url_key', u'phone', u'subkind']
  update_row = [u'name', u'kana', u'address', u'phone', u'parent_url_key', u'subkind', u'url', u'serial_id']

  @classmethod
  def dump_hotel(cls, name, action='normal'):
    from ghost_spider.elastic import LocationHotelEs, LatteHotelEs
    from ghost_spider import progressbar

    filename = cls.get_filename_by_name(name)
    query = {"query": {"bool": {"must": [{"term": {"prefecture_ascii": name}}], "must_not": []}}}

    if action == 'recover':
      query["query"]["bool"]["must"].append({"term": {"recovered": "1"}})
      filename = cls.get_filename_by_name(u'%s_recover' % name)
    elif action == 'production':
      filename = cls.get_filename_by_name(u'%s_production' % name)
      query["query"]["bool"]["must"].append({"term": {"version": 10}})

    query["query"]["bool"]["must_not"].append({"term": {"genre": u'ラブホテル'}})

    if os.path.exists(filename):
      os.remove(filename)

    progress = None
    total = 0
    page = 1
    limit = 100
    sort = [{"area.untouched": "asc"}]

    save_data_to_file = cls.save_for_production if action == u'production' else cls.save_to_csv

    print "=" * 100
    while True:
      places, total = LocationHotelEs.pager(query=query, page=page, size=limit, sort=sort)
      page += 1
      if not places or not len(places):
        break
      if not progress:
        print "Dumping data for %s (%s)" % (name, total)
        progress = progressbar.AnimatedProgressBar(end=total, width=100)
      progress + limit
      progress.show_progress()
      for place in places:
        result = LatteHotelEs.get_place_by_name(place.get('name'))
        if result["hits"]["total"] > 0:
          place["latte_url"] = result["hits"]["hits"][0]["_source"]["url"]

        if action == 'normal':
          hotel_kind = u'ホテル'
          if place.get('kind') and place.get('kind') in LocationHotelSelectors.REPLACE_HOTEL:
            hotel_kind = place.get('kind')
          else:
            for genre in place['genre']:
              if genre in LocationHotelSelectors.REPLACE_HOTEL:
                hotel_kind = LocationHotelSelectors.REPLACE_HOTEL[genre]
                break
          place['kind'] = hotel_kind
        save_data_to_file(filename, place)
    print " "

  @classmethod
  def dump_restaurant(cls, name, action=None):
    from ghost_spider.elastic import LocationRestaurantEs, LatteRestaurantEs
    from ghost_spider.data import URL_TARGET_URLS
    from ghost_spider import progressbar
    from ghost_spider.data import RST_KINDS_LATE_NOT_ALLOWED

    query = {"query": {"bool": {"must": [{"term": {"prefecture_ascii": name}}], "must_not": []}}}
    if action == 'production':
      query["query"]["bool"]["must"].append({"term": {"version": 10}})

    query["query"]["bool"]["must_not"].append({"terms": {"genre.untouched": RST_KINDS_LATE_NOT_ALLOWED.keys()}})

    progress = None
    total = 0
    page = 1
    limit = 100
    sort = [{"area.untouched": "asc"}]

    save_data_to_file = cls.save_for_production if action == u'production' else cls.save_to_csv

    print "=" * 100
    count_lines = 0

    while True:
      places, total = LocationRestaurantEs.pager(query=query, page=page, size=limit, sort=sort)
      page += 1
      if not places or not len(places):
        break
      if not progress:
        print "Dumping data for %s (%s)" % (name, total)
        progress = progressbar.AnimatedProgressBar(end=total, width=100)
      progress + limit
      progress.show_progress()
      for place in places:
        result = LatteRestaurantEs.get_place_by_name(place.get('name'))
        if result["hits"]["total"] > 0:
          place["latte_url"] = result["hits"]["hits"][0]["_source"]["url"]
          place["latte_url"] = place["latte_url"].replace(URL_TARGET_URLS[0], URL_TARGET_URLS[1])

        place['kind'] = u'|'.join(place['kind'])
        if count_lines % 10000 == 0:
          count = (count_lines / 10000) + 1
          filename = cls.get_filename_by_name(name, count=count, remove_file=True)

        count_lines += 1
        save_data_to_file(filename, place)
    print " "

  @classmethod
  def save_to_csv(cls, filename, data):
    """Save this data on csv file by prefecture"""
    row = []

    address = zenhan.z2h(data['address'], zenhan.ASCII)
    # remove the zip code
    address = re.sub(r'%s\d+-\d+' % u'〒', '', address).strip()
    row.append(data['name'])
    row.append(data['name_kata'])
    row.append(address)

    row.append(data['prefecture'])
    row.append(data['area'])

    row.append(zenhan.z2h(data['phone'], zenhan.ALL))

    row.append(data['kind'])

    row.append(data.get('latte_url') or u'')
    row.append(data['page_url'])
    row.append(data['id'])

    CsvWriter.write_to_csv(filename, row, firs_row=cls.first_row)

  @classmethod
  def save_for_production(cls, filename, data):
    """Save this data on csv file by prefecture"""
    row = []

    address = zenhan.z2h(data['address'], zenhan.ALL)
    # remove the zip code
    address = re.sub(r'%s\d+-\d+' % u'〒', '', address).strip()
    row.append(data['name'])
    row.append(data['name_kata'])
    row.append(address)
    row.append(data['parent_url_key'])
    row.append(zenhan.z2h(data['phone'], zenhan.ALL))
    # hotel_kind = u'ホテル'
    # if data.get('kind') and data.get('kind') in LocationHotelSelectors.REPLACE_HOTEL:
    #   hotel_kind = data.get('kind')
    # else:
    #   for genre in data['genre']:
    #     if genre in LocationHotelSelectors.REPLACE_HOTEL:
    #       hotel_kind = LocationHotelSelectors.REPLACE_HOTEL[genre]
    #       break
    row.append(data.get('kind') or '')
    CsvWriter.write_to_csv(filename, row, firs_row=cls.production_first_row)

  @classmethod
  def get_filename_by_name(cls, name, count=0, remove_file=False):
    directory = u'%s/%s' % (cls.TARGET_DIR_FORMAT, name)
    if not os.path.exists(directory):
        os.makedirs(directory)

    if count:
      filename = os.path.join(directory, u'%s_%s.csv' % (name, count))
    else:
      filename = os.path.join(directory, u'%s.csv' % name)

    if remove_file and os.path.exists(filename):
      os.remove(filename)
    return filename

  @classmethod
  def import_file(cls, csvfile, kind):
    """Save in database the csv handler.

    csvfile: fileHandler

    """
    import csv
    import progressbar
    import time
    from ghost_spider.elastic import LocationHotelEs, LocationRestaurantEs
    reader = csv.DictReader(csvfile, cls.update_row)
    if not kind in ['hotel', 'restaurant']:
      raise NotImplementedError()
    classHolder = LocationHotelEs
    if kind == 'restaurant':
      classHolder = LocationRestaurantEs
    classHolder.DEBUG = False
    next(reader)  # skip the title line
    rows = list(reader)
    total = len(rows)
    progress = progressbar.AnimatedProgressBar(end=total, width=100)
    bulk = ""
    count_lines = 0
    for line, row in enumerate(rows):
      progress += 1
      progress.show_progress()
      sanitized = {}
      for k, v in row.iteritems():
        if v:
          if not isinstance(v, (list, tuple)):
            sanitized.update({k: v.decode('utf-8').strip()})
      if not sanitized.get('parent_url_key'):
        continue
      # url = sanitized['url']
      # result = classHolder.get_place_by_url(url)
      ids = [sanitized['serial_id']]
      result = classHolder.get_place_by_ids(ids)
      if result["hits"]["total"] > 0:
        data = result["hits"]["hits"][0]["_source"]
        data_id = result["hits"]["hits"][0]["_id"]
        data['name_low'] = sanitized['name'].lower().strip()
        data['name'] = sanitized['name']
        data['name_kata'] = sanitized.get('kana') or u''
        data['address'] = sanitized['address']
        data['phone'] = sanitized.get('phone') or u''
        data['parent_url_key'] = sanitized['parent_url_key']
        data['kind'] = sanitized['subkind'].split('|') if sanitized.get('subkind') else []
        data['version'] = 10
        bulk += classHolder.bulk_data(data, data_id=data_id, action="update")
        if (count_lines % 100) == 0:
          classHolder.send(bulk)
          bulk = ""
        count_lines += 1

    if bulk:
      classHolder.send(bulk)
    progress += total + 1
    progress.show_progress()
    print " "
    print count_lines
