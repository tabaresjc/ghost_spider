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

  def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
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
  LOG_OUTPUT_FILE_INFO = "upload/info-log.txt"
  ES_SORT = [
    {
      "area2.untouched": "asc"
    },
    {
      "popularity": "desc"
    }
  ]
  TARGET_DIR_FORMAT = u'upload/crawler/hotels/%s/%s'
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

  @property
  def logger(self):
    """error log handler."""
    try:
      return self._logger
    except AttributeError:
      self._logger = logging.getLogger(self.name)
      ch = logging.FileHandler(self.LOG_OUTPUT_FILE_INFO)
      formatter = logging.Formatter('%(asctime)s - %(message)s')
      ch.setFormatter(formatter)
      ch.setLevel(logging.INFO)
      self._logger.addHandler(ch)
      self.total_count = 0L
    return self._logger

  def dump(self):
    from ghost_spider.elastic import LocationHs
    page = 1
    limit = 100
    shutil.rmtree('upload/')
    while True:
      places, total = LocationHs.pager(page=page, size=limit, sort=self.ES_SORT)
      page += 1
      if not places or not len(places):
        break
      for p in places:
        self.logger.info(u'Saving %s > %s > %s' % (p.get('area1'), p.get('area2'), p['place'][0]['name']))
        self.save_to_csv(p)

  def save_to_csv(self, item):
    filename = self.get_filename(item, u'hotels.csv')
    row = []

    for p in item['place']:
      for k, v in p.iteritems():
        if k == 'lang':
          continue
        nk = u'%s_%s' % (k, p['lang']) if p['lang'] != 'en' else k
        item[nk] = v

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
    directory = self.TARGET_DIR_FORMAT % (item.get('area1'), item.get('area2'))
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = u'%s/%s' % (directory, filename)
    return filename
