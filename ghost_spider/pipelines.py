# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from ghost_spider import helper
from ghost_spider.helper import clean_lf, rev_telephone
from ghost_spider.util import CsvWriter
from helper import debug_screen
from elastic import PlaceHs


class GhostSpiderPipeline(object):

  """Process & format data after being scrapped from page."""

  def process_item(self, item, spider):
    item_es = {}
    for k, v in item.iteritems():
      if k == 'phone':
        if v and len(v):
          v = helper.SEL_RE_PHONE_NUMBER.findall(v[0])
        item[k] = rev_telephone(v[0] if len(v) else u'')
      elif k == 'amenity':
        item[k] = clean_lf(v, u', ')
      elif k == 'page_breadcrumbs':
        vlen = len(v)
        item[k] = v[:3] if vlen > 3 else v
      elif k.startswith('page_body'):
        pass
      else:
        item[k] = clean_lf(v)
      item_es[k] = item[k]
    item_es['name_low'] = item['name'].lower()
    item_es['rating'] = float(item['rating'] or 0)
    item_es['popularity'] = float(item['popularity'] or 0)
    item_es['page_url'] = item_es['page_url'].lower()
    
    if not PlaceHs.check_by_name(item['name']):
      PlaceHs.save(item_es)
    self.save_to_csv(item_es)
    return item

  def save_to_csv(self, item):
    from ghost_spider.settings import CSV_OUTPUT_FILE
    row = []
    row.append(item['name'])
    row.append(item['name_ja'])
    row.append(item['name_es'])
    row.append(item['name_zh'])
    CsvWriter.write_to_csv(CSV_OUTPUT_FILE, row)
