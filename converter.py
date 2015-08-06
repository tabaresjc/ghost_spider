# -*- coding: utf-8 -*-
import sys
from ghost_spider.settings import setup_elastic_connection as get_es_connection


def export_csv_file(kind, name, action=None):
  from ghost_spider.util import LocationCsv
  if kind == 'hotel':
    LocationCsv.dump_hotel(name, action=action)
  elif kind == 'restaurant':
    LocationCsv.dump_restaurant(name, action=action)
  else:
    NotImplementedError()


def import_csv_file(filename, kind):
  """Store data from csv files.

  filename: string
  kind: string (hotel/restaurant)

  """
  from ghost_spider.util import CsvImporter
  CsvImporter.import_file(filename, kind)


def update_location_from_file(filename, kind):
  import os
  from ghost_spider.util import LocationCsv

  f = open(filename, 'rb')
  try:
    LocationCsv.import_file(f, kind)
  finally:
    if f:
      f.close()


def index_elastic(index, action="create", config_file=None):
  """Create index or force i.e. delete if exist then create it.

  action: str [create or force]
  config_file: str

  """
  es = get_es_connection()

  if not config_file:
    config_file = index
  file_name = 'schema/%s/index_%s.json' % (index, config_file)
  # create if not exist
  if action == "create":
    try:
      es.request(method="get", myindex=index, mysuffix="_settings")
    except:
      es.request(method="post", myindex=index, jsonnize=False, mydata=_read_schema(file_name))
  elif action == "force":
    # force creation i.e delete and then create
    try:
      es.request(method="delete", myindex=index)
    except:
      pass
    es.request(method="post", myindex=index, jsonnize=False, mydata=_read_schema(file_name))


def type_elastic(index, type, action=None):
  """Create type with mapping or merge the mapping.

  type: str
  index: str
  action: str

  """
  es = get_es_connection()
  # delete the if exist
  if action == "force":
    try:
      es.request(method="delete", myindex=index, mytype=type)
    except:
      pass
  file_name = 'schema/%s/mapping_%s.json' % (index, type)
  # create if not exist or merge
  es.request(method="post", myindex=index, mytype=type, mysuffix="_mapping", jsonnize=False, mydata=_read_schema(file_name))


def type_merge(name):
  """Merge type, experimental only use if you are sure that you can get your data back (i.e reindex).

  It's to avoid re-indexing when you have a merge conflict.

  name: str Name of the model you want to merge

  """
  # import dynamically the
  mod = __import__("model", fromlist=[name])
  type_es = getattr(mod, name)
  es = get_es_connection()
  tmp_type = "%s2" % type_es.type
  # copy type in type2 force creation
  file_name = 'schema/%s/mapping_%s.json' % (type_es.index, type_es.type)
  es.request(method="post", myindex=type_es.index, mytype=tmp_type, mysuffix="_mapping", jsonnize=False, mydata=_read_schema(file_name))
  _copy_type(type_es, type_es.type, tmp_type)
  type_elastic(type_es.index, type_es.type, "force")
  _copy_type(type_es, tmp_type, type_es.type)
  es.request(method="delete", myindex=type_es.index, mytype=tmp_type)


def replicate_type():
  """Duplicate a type from type_from to type_to."""
  # build bulk
  from ghost_spider.elastic import LocationRestaurantEs
  from ghost_spider import progressbar
  size = 100
  page = 0
  progress = None
  total = 0
  while True:
    start_from = page * size
    results = LocationRestaurantEs.search({"query": {"match_all": {}}, "size": size, "from": start_from})
    if not progress:
      total = results["hits"]["total"]
      progress = progressbar.AnimatedProgressBar(end=total, width=100)

    if not results["hits"]["hits"]:
      break

    page += 1
    bulk = ""
    for result in results["hits"]["hits"]:
      data = result["_source"]
      bulk += LocationRestaurantEs.bulk_data(data, type_name="restaurants_back")
    progress + size
    progress.show_progress()
    LocationRestaurantEs.send(bulk)
  if progress:
    progress + total
    progress.show_progress()
  print "total %s" % total


def update_location():
  """Duplicate a type from type_from to type_to."""
  # build bulk
  from ghost_spider.elastic import LocationRestaurantEs
  from ghost_spider.helper import LocationHotelSelectors
  from ghost_spider import progressbar
  import re
  size = 100
  page = 0
  progress = None
  total = 0
  query = {"query": {"match_all": {}}, "size": size, "from": 0}
  query["sort"] = [{"name.untouched": "asc"}]
  while True:
    query["from"] = page * size
    results = LocationRestaurantEs.search(query)
    if not progress:
      total = results["hits"]["total"]
      print "total %s" % total
      progress = progressbar.AnimatedProgressBar(end=total, width=100)
    if not results["hits"]["hits"]:
      break

    page += 1
    bulk = ""
    for result in results["hits"]["hits"]:
      location = result["_source"]
      data_id = result["_id"]
      genre = []
      area_found = False
      for a in location["page_body"]["genre"]:
        result = re.findall(r'genrecd=\d+', a)
        if u'genrecd=01' in result:
          if not area_found:
            text = re.findall(r'>(.*)</', a)
            location['area'] = text[0]
            if location['area']:
              location['area_ascii'] = LocationRestaurantEs.analyze(location['area'], 'romaji_ascii_normal_analyzer')
            area_found = True
        else:
          text = re.findall(r'>(.*)</', a)
          if text and text[0]:
            genre.append(text[0])
      if not area_found and len(location["page_body"]["breadcrumbs"]) > 1:
        location['area'] = location["page_body"]["breadcrumbs"][-1]
        if location['area']:
          location['area_ascii'] = LocationRestaurantEs.analyze(location['area'], 'romaji_ascii_normal_analyzer')
      elif not area_found:
        location['area'] = ''
        location['area_ascii'] = ''
      kind = LocationHotelSelectors.convert_latte_kind(genre)
      location['genre'] = genre
      location['kind'] = kind
      bulk += LocationRestaurantEs.bulk_data(location, action="update", data_id=data_id)
    progress + size
    progress.show_progress()
    LocationRestaurantEs.send(bulk)
  if progress:
    progress + total
    progress.show_progress()
  print " "


def delete_type(index, type):
  """delete a type from index."""
  es = get_es_connection()
  es.request(method="delete", myindex=index, mytype=type)


def _copy_type(type_es, type_from, type_to):
  """Duplicate a type from type_from to type_to."""
  type_es.type = type_from
  # build bulk
  size = 100
  page = 0
  while True:
    start_from = page * size
    results = type_es.search({"query": {"match_all": {}}, "size": size, "from": start_from})
    if not results["hits"]["hits"]:
      break
    page += 1
    bulk = ""
    for result in results["hits"]["hits"]:
      bulk += type_es.bulk_data(result["_source"], type_name=type_to)
    type_es.send(bulk)


def elastic_backup(name=None):
  """Launch a snapshot with generated name directory if not provided.

  name: str

  """
  es = get_es_connection()
  import datetime

  if not name:
    name = "backup"
  _check_repository(es, name)

  snapshot_name = datetime.datetime.now().strftime("%Y%m%d%I%M%S")
  print es.request(method="put", mysuffix="_snapshot/%s/%s?wait_for_completion=true" % (name, snapshot_name))


def _check_repository(es, name):
  """Check if the repository exist if not create it."""
  try:
    es.request(method="get", mysuffix="_snapshot/%s" % name)
  except:
    repository = {"type": "fs",
      "settings": {
        "location": "/tmp/%s" % name,
        "compress": True
      }
    }
    es.request(method="put", mysuffix="_snapshot/%s" % (name), mydata=repository)


def create_repository(name):
  """create repository."""
  es = get_es_connection()
  repository = {"type": "fs",
    "settings": {
      "location": "/tmp/%s" % name,
      "compress": True
    }
  }
  es.request(method="put", mysuffix="_snapshot/%s" % (name), mydata=repository)


def _read_schema(filename):
  """Read the content of a file.

  filename: string

  return: content of the file

  """
  import os
  project_root = os.path.abspath(os.path.dirname(__file__))
  f = open(os.path.join(project_root, filename), 'rb')
  try:
    return f.read()
  finally:
    if f:
      f.close()


def main():
  from optparse import OptionParser
  parser = OptionParser()
  parser.add_option('--id', type='int')
  parser.add_option('--name', type='str')
  parser.add_option('--filename', type='str')
  parser.add_option('--kind', type='str')
  parser.add_option('--file', type='str')
  parser.add_option('--action', type='str')
  parser.add_option('--index', type='str')
  parser.add_option('--type', type='str')
  parser.add_option('--delta', type='int')
  parser.add_option('--ignore', type='str')
  parser.add_option('--resume', type='str')
  options, args = parser.parse_args()
  if 1 != len(args):
    parser.error('empty command.')
    return

  options = dict([(k, v) for k, v in options.__dict__.iteritems() if not k.startswith('_') and v is not None])

  commands = {
    'index_elastic': index_elastic,
    'type_elastic': type_elastic,
    'elastic_backup': elastic_backup,
    'type_merge': type_merge,
    'replicate_type': replicate_type,
    'delete_type': delete_type,
    'update_location': update_location,
    'import_csv_file': import_csv_file,
    'export_csv_file': export_csv_file,
    'update_location_from_file': update_location_from_file
  }
  command = commands[args[0]]

  try:
    command(**options)
  finally:
    pass

if __name__ == '__main__':
  main()
