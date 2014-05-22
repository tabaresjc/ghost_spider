# -*- coding: utf-8 -*-

import json
import datetime
from settings import es


class Elastic(object):
  """Manage all the actions to ElasticSearch"""

  min_score = 0.0005
  index = None
  type = None
  default_analyzer = None
  debug = False

  @classmethod
  def get_connection(cls):
    """Create the connection to ElasticSearch"""
    return es

  @classmethod
  def update(cls, my_id, my_data, method="post", mysuffix="_update"):
    """Make the job.

    my_id: int id of the object you want to update.
    my_data: dict new data to add.
    method: str
    mysuffix: str

    return: ElasticSearch result.

    """
    conn = cls.get_connection()
    return conn.request(
      method=method, myindex=cls.index, mytype=cls.type, mysuffix=mysuffix,
      myID=my_id, mydata=my_data
    )

  @classmethod
  def save(cls, my_data, create=False):
    """Make the job.

    my_data: dict
    create: bool true for create false to update.

    """
    my_params = {}
    if create:
      my_params = {"op_type": "create"}

    #check json data
    json.dumps(my_data)

    conn = cls.get_connection()
    conn.request(
        method="post",
        myindex=cls.index,
        mytype=cls.type,
        myID=my_data.get("id") or '',
        myparams=my_params,
        mydata=my_data
    )

  @classmethod
  def delete(cls, data):
    """support Delete and Delete by query API

    data: dict or int

    data = 1 if you want to delete the document 1
    data = {"id": 1} if you want to delete the id document
    data = {"query": {"term": {"user_id": 2}}}
    data = {"query": {"terms": {"id": [2,3,4,5]}}}

    """
    conn = cls.get_connection()
    if(isinstance(data, int)):
      conn.request(method="delete", myindex=cls.index, mytype=cls.type, myID=data)
    elif data.get("id"):
      conn.request(method="delete", myindex=cls.index, mytype=cls.type, myID=data["id"])
    elif data.get("query"):
      conn.request(method="delete", myindex=cls.index, mytype=cls.type, mysuffix="_query", mydata=data)

  @classmethod
  def refresh(cls):
    """Force the refresh of the index when you insert, delete or update and search after"""
    conn = cls.get_connection()
    conn.request(method="post", myindex=cls.index, mysuffix="_refresh")

  @classmethod
  def analyze(cls, data, analyzer):
    """Make an analyze request.

    data: dict data to be analyzed.
    analyzer: str name of the analyzer we'll use.

    return: ElasticSearch result.

    """
    import urllib2
    import server

    result = json.loads(urllib2.urlopen(u'http://%s/%s/_analyze?analyzer=%s' % (server.server.config['ELASTICSEARCH_SERVER'][0], cls.index, analyzer), data.encode('utf-8')).read())
    value = []
    for token in result["tokens"]:
      value.append(token["token"])

    return ' '.join(value)

  @classmethod
  def search(cls, query, suffix=None, pager=False):
    """Perform the search.

    query: dict
    suffix: string
    pager: bool, wrap the data in a list more easy to fetch for pagination

    return: ElasticSearch result.

    """
    debug_level = False
    # debug_level = 1 #only the query
    # debug_level = 2 #only the result
    # debug_level = 3 #both

    conn = cls.get_connection()
    my_suffix = "_search"
    result = {}

    if debug_level in [1, 3]: # pragma: no cover
      print "/-" * 42
      print json.dumps(query)
      print "-/" * 42

    if suffix:
      my_suffix = suffix

    result = conn.request(
      method="post", myindex=cls.index, mytype=cls.type,
      mysuffix=my_suffix, mydata=query
    )

    if debug_level in [2, 3]: # pragma: no cover
      print "~*" * 42
      print result
      print "~*" * 42

    if pager:
      records = []
      total = result["hits"]["total"]
      if total:
        for r in result["hits"]["hits"]:
          record = r["_source"]
          record["id"] = r["_id"]
          records.append(record)
      return records, total

    return result

  @classmethod
  def send(cls, bulk):
    """Send a bulk.

    bulk: dict

    return: ElasticSearch result.

    """
    conn = cls.get_connection()
    return conn.request(
      method="post", mysuffix="_bulk", mydata=bulk, jsonnize=False)

  @classmethod
  def build_date_range(cls, value, field):
    """Build a query date range.

    value: dict or string
    field: str

    return: the updated query.
    """
    start_hour = "00:00:00"
    end_hour = "23:59:59"
    start_date = None
    end_date = None
    if isinstance(value, dict):
      for key, date in value.iteritems():
        try:#the date must be formatted as yyyy-mm-dd or yyyy-mm-dd hh:mm:ss
          datetime.datetime.strptime(date, '%Y-%m-%d')
          if key == "gte":
            start_date = date + " " + start_hour
          elif key == "lte":
            end_date = date + " " + end_hour
        except ValueError:
          try:
            datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            if key == "gte":
              start_date = date
            elif key == "lte":
              end_date = date
          except ValueError:
            raise ValueError("Incorrect date format, should be YYYY-MM-DD or YYYY-MM-DD hh:mm:ss" + date)
    else:
      try:#the date must be formatted as yyyy-mm-dd
        datetime.datetime.strptime(value, '%Y-%m-%d')
      except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")
      start_date = value + " " + start_hour
      end_date = value + " " + end_hour

    if start_date and end_date:
      field_range = {"gte": start_date, "lte": end_date}
    elif start_date:
      field_range = {"gte": start_date}
    elif end_date:
      field_range = {"lte": end_date}

    query = {"range": {
      field: field_range
    }}
    return query

  @classmethod
  def pager(cls, query={"query": {"match_all": {}}}, page=1, size=20, sort=None):
    """Get records on the selected page.

    query dict
    page: int
    size: int
    sort: dict

    return: records, total

    """
    #build the query
    my_query = query

    if sort:
      my_query["sort"] = sort

    #pagination
    my_query["from"] = (page - 1) * size
    my_query["size"] = size

    return cls.search(query=my_query, pager=True)

  @classmethod
  def count(cls):
    """Get the total of records."""
    conn = cls.get_connection()
    result = conn.request(myindex=cls.index, mytype=cls.type, mysuffix="_count", jsonnize=False)
    return result["count"]

  @classmethod
  def bulk_data(cls, data, action="create", type_name=None):
    """Build a bulk from data.

    data: dict
    action: any of [create, update]
    type_name: str

    return: formatted data for bulk in Json.

    """
    if not type_name:
      type_name = cls.type
    bulk_header = {
      action: {
        "_index": cls.index,
        "_type": type_name
      }
    }

    #for some data I don't have id
    if data.get("id"):
      bulk_header[action]["_id"] = data["id"]

    if action == "update":
      data = {"doc": data}
    return json.dumps(bulk_header) + '\n' + json.dumps(data) + '\n'


class PlaceHs(Elastic):

  """List of cities area for Hair Salon."""

  index = "travel_crawler"
  type = "places"

  @classmethod
  def save(cls, data):
    """Save the area.

      data: dict

    """
    super(PlaceHs, cls).save(data, True)

  @classmethod
  def get_by_id(cls, id):
    """Get an area by his id.

    id: string

    return: ElasticSearch result

    """
    area = None
    if not id:
      return area
    result = cls.search({"query": {"match_all": {}}, "post_filter": {"term": {"_id": id}}})
    if result["hits"]["total"] == 1:
      area = result["hits"]["hits"][0]["_source"]
      area["id"] = result["hits"]["hits"][0]["_id"]

    return area
