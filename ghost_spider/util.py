# -*- coding: utf-8 -*-
import os.path
import csv, codecs, cStringIO
import shutil
import os
import logging


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
    from ghost_spider.elastic import LocationHs
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
      places, total = LocationHs.pager(query=query, page=page, size=limit, sort=sort)
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
