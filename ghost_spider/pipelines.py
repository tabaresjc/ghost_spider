# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from ghost_spider.helper import clean_lf, rev_telephone


class GhostSpiderPipeline(object):

  """Process & format data after being scrapped from page."""

  def process_item(self, item, spider):
    for k, v in item.iteritems():
      if k == 'phone':
        item[k] = rev_telephone(v[0] if len(v) else u'')
      elif k == 'amenity':
        item[k] = clean_lf(v, u', ')
      else:
        item[k] = clean_lf(v)
    return item
