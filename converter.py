# -*- coding: utf-8 -*-
import sys
from ghost_spider.settings import setup_elastic_connection as get_es_connection


def remove_hotels():
  from ghost_spider.elastic import LocationHs
  hotels = [line.strip() for line in open("output/erase_hotels.txt", "r")]
  count = 0
  print ".*" * 50
  for name in hotels:
    result = LocationHs.get_place_by_name(name, fields=['name'])
    if result["hits"]["total"] == 1:
      count += 1
      print u'%s' % name
      LocationHs.delete({'id': result["hits"]["hits"][0]["_id"]})


def export_hotels_to_csv(name):
  from ghost_spider.util import LocationHotelsToCsvFiles
  exporter = LocationHotelsToCsvFiles(name)
  exporter.dump()


def export_salons_to_csv(name, action=None):
  from ghost_spider.util import SalonToCsvFiles
  exporter = SalonToCsvFiles(name)
  exporter.dump(action=action)


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
  from ghost_spider.elastic import SalonEs
  from ghost_spider import progressbar
  size = 100
  page = 0
  progress = None
  total = 0
  while True:
    start_from = page * size
    results = SalonEs.search({"query": {"match_all": {}}, "size": size, "from": start_from})
    if not progress:
      total = results["hits"]["total"]
      progress = progressbar.AnimatedProgressBar(end=total, width=100)

    if not results["hits"]["hits"]:
      break

    page += 1
    bulk = ""
    for result in results["hits"]["hits"]:
      shop = result["_source"]
      if shop.get('page_url'):
        shop['page_url'] = shop['page_url'].lower()
      bulk += SalonEs.bulk_data(result["_source"], type_name="shops")
    progress + size
    progress.show_progress()
    SalonEs.send(bulk)
  if progress:
    progress + total
    progress.show_progress()
  print "total %s" % total


def update_shop():
  """Duplicate a type from type_from to type_to."""
  # build bulk
  from ghost_spider.elastic import SalonEs
  from ghost_spider import progressbar
  size = 100
  page = 0
  progress = None
  total = 0
  query = {"query": {"bool": {"must": [{"terms": {"prefecture.untouched": [u'京']}}]}}, "size": size, "from": 0}
  # query = {"query": {"match_all": {}}, "size": size, "from": 0}
  while True:
    query["from"] = page * size
    results = SalonEs.search(query)
    if not progress:
      total = results["hits"]["total"]
      print "total %s" % total
      progress = progressbar.AnimatedProgressBar(end=total, width=100)
    if not results["hits"]["hits"]:
      break

    page += 1
    bulk = ""
    for result in results["hits"]["hits"]:
      shop = result["_source"]
      data_id = result["_id"]
      shop['prefecture'] = u'京都'
      shop['prefecture_ascii'] = u'kyoto'
      bulk += SalonEs.bulk_data(shop, type_name="shops", action="update", data_id=data_id)
    progress + size
    progress.show_progress()
    SalonEs.send(bulk)
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
  PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
  f = open(os.path.join(PROJECT_ROOT, filename), 'rb')
  try:
    return f.read()
  finally:
    if f:
      f.close()


def remove_restaurants_from_hotel():
  from ghost_spider.elastic import LocationHs
  from ghost_spider import progressbar
  query = {
    "query": {
      "bool": {
        "must": [{"wildcard": {"place.page_url": "*restaurant*"}}],
        "must_not": [{"wildcard": {"place.page_url": "*hotel*"}}]
      }
    }
  }
  progress = None
  limit = 100
  page = 1
  places, total = LocationHs.pager(query, page=page, size=limit)
  print "Finding restaurants withing hotel"
  while True:
    if total <= 0:
      break
    if not progress:
      progress = progressbar.AnimatedProgressBar(end=total, width=50)

    progress + limit
    progress.show_progress()
    places, tot = LocationHs.pager(query, page=page, size=limit)
    if not places or not len(places):
      break
    for p in places:
      LocationHs.delete({'id': p['id']})
    total -= limit

  print "Finito!!!"


def main():
  from optparse import OptionParser
  parser = OptionParser()
  parser.add_option('--id', type='int')
  parser.add_option('--name', type='str')
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
    'export_hotels_to_csv': export_hotels_to_csv,
    'export_salons_to_csv': export_salons_to_csv,
    'remove_hotels': remove_hotels,
    'remove_restaurants_from_hotel': remove_restaurants_from_hotel,
    'replicate_type': replicate_type,
    'delete_type': delete_type,
    'update_shop': update_shop
  }
  command = commands[args[0]]

  try:
    command(**options)
  finally:
    pass

if __name__ == '__main__':
  main()
