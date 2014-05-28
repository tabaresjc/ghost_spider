# -*- coding: utf-8 -*-

from ghost_spider.settings import setup_elastic_connection as get_es_connection


def fix_data_mistake():
  """Fix data saved in a wrong way."""
  from ghost_spider.elastic import PlaceHs
  page = 1
  limit = 1000
  while True:
    places, total = PlaceHs.pager(page=page, size=limit)
    page += 1
    if not places or not len(places):
      print "*-" * 50
      print "Finito!!!!"
      print "*-" * 50
      break
    bulk = u''
    for p in places:
      print u'Update %s > %s > %s' % (p.get('area1'), p.get('area2'), p['place'][0]['name'])
      p['region'] = p['place'][0]['address_region']
      new_place = []
      for el in p['place']:
        new_el = {}
        for k, v in el.iteritems():
          if k.startswith('lang'):
            new_el['lang'] = v
          elif k.startswith('name'):
            new_el['name'] = v
          elif k.startswith('amenity'):
            new_el['amenity'] = v
          elif k.startswith('address_locality'):
            new_el['address_locality'] = v
          elif k.startswith('address_area_name'):
            new_el['address_area_name'] = v
          elif k.startswith('address_region'):
            new_el['address_region'] = v
          elif k.startswith('address_street'):
            new_el['address_street'] = v
          elif k.startswith('address_zip'):
            new_el['address_zip'] = v
          elif k.startswith('page_body'):
            new_el['page_body'] = v
        new_place.append(new_el)
      p['place'] = new_place
      bulk += PlaceHs.bulk_place(p)
    result = PlaceHs.send(bulk)
    for doc_missing in result["items"]:
      if doc_missing["update"].get("error"):
        print "error updating..."


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
    except Exception:
      es.request(method="post", myindex=index, jsonnize=False, mydata=_read_schema('data/elastic/index_%s.json' % config_file))
  elif action == "force":
    #force creation i.e delete and then create
    try:
      es.request(method="delete", myindex=index)
    except Exception:
      pass
    es.request(method="post", myindex=index, jsonnize=False, mydata=_read_schema('data/elastic/index_%s.json' % config_file))


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
  es.request(method="post", myindex=index, mytype=type, mysuffix="_mapping", jsonnize=False, mydata=_read_schema('data/elastic/mapping_%s.json' % type))


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
  es.request(method="post", myindex=typeEs.index, mytype=tmp_type, mysuffix="_mapping", jsonnize=False, mydata=_read_schema('data/elastic/mapping_%s.json' % typeEs.type))
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
  total = typeEs.count()
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
  except Exception, e:
    if "RepositoryMissingException" in e[0]:
      repository = {"type": "fs",
        "settings": {
          "location": "/tmp/%s" % name,
          "compress": True
        }
      }
      es.request(method="put", mysuffix="_snapshot/%s" % (name), mydata=repository)
    else:
      print "other problem (the repository is here but...)"
      raise


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
