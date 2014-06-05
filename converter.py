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


def export_hotels_to_csv():
  from ghost_spider.util import LocationHotelsToCsvFiles
  exporter = LocationHotelsToCsvFiles()
  exporter.dump()


def index_elastic(index, action="create", config_file=None):
  """Create index or force i.e. delete if exist then create it.

  action: str [create or force]
  config_file: str

  """
  es = get_es_connection()

  if not config_file:
    config_file = index

  #create if not exist
  if action == "create":
    try:
      es.request(method="get", myindex=index, mysuffix="_settings")
    except:
      es.request(method="post", myindex=index, jsonnize=False, mydata=_read_schema('schema/index_%s.json' % config_file))
  elif action == "force":
    #force creation i.e delete and then create
    try:
      es.request(method="delete", myindex=index)
    except:
      pass
    es.request(method="post", myindex=index, jsonnize=False, mydata=_read_schema('schema/index_%s.json' % config_file))


def type_elastic(index, type, action=None):
  """Create type with mapping or merge the mapping.

  type: str
  index: str
  action: str

  """
  es = get_es_connection()
  #delete the if exist
  if action == "force":
    try:
      es.request(method="delete", myindex=index, mytype=type)
    except:
      pass

  #create if not exist or merge
  es.request(method="post", myindex=index, mytype=type, mysuffix="_mapping", jsonnize=False, mydata=_read_schema('schema/mapping_%s.json' % type))


def type_merge(name):
  """Merge type, experimental only use if you are sure that you can get your data back (i.e reindex).
  
  It's to avoid re-indexing when you have a merge conflict.

  name: str Name of the model you want to merge

  """
  #import dynamically the
  mod = __import__("model", fromlist=[name])
  typeEs = getattr(mod, name)
  es = get_es_connection()
  tmp_type = "%s2" % typeEs.type
  #copy type in type2 force creation
  es.request(method="post", myindex=typeEs.index, mytype=tmp_type, mysuffix="_mapping", jsonnize=False, mydata=_read_schema('schema/mapping_%s.json' % typeEs.type))
  _copy_type(typeEs, typeEs.type, tmp_type)
  type_elastic(typeEs.index, typeEs.type, "force")
  _copy_type(typeEs, tmp_type, typeEs.type)
  es.request(method="delete", myindex=typeEs.index, mytype=tmp_type)


def _copy_type(typeEs, type_from, type_to):
  """Duplicate a type from type_from to type_to."""
  typeEs.type = type_from
  #build bulk
  size = 100
  page = 0
  while True:
    start_from = page * size
    results = typeEs.search({"query": {"match_all": {}}, "size": size, "from": start_from})
    if not results["hits"]["hits"]:
      break
    page += 1
    bulk = ""
    for result in results["hits"]["hits"]:
      bulk += typeEs.bulk_data(result["_source"], type_name=type_to)
    typeEs.send(bulk)


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

  COMMANDS = {
    'index_elastic': index_elastic,
    'type_elastic': type_elastic,
    'elastic_backup': elastic_backup,
    'type_merge': type_merge,
    'export_hotels_to_csv': export_hotels_to_csv,
    'remove_hotels': remove_hotels
  }
  command = COMMANDS[args[0]]

  try:
    command(**options)
  finally:
    pass

if __name__ == '__main__':
  main()
