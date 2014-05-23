# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from ghost_spider import helper
from ghost_spider.util import CsvWriter
from elastic import PlaceHs


class GhostSpiderPipeline(object):

  """Process & format data after being scrapped from page."""

  def process_item(self, item, spider):
    item_es = {}
    for k, v in item.iteritems():
      if k == 'phone':
        if v and len(v):
          v = helper.SEL_RE_PHONE_NUMBER.findall(v[0])
        item[k] = helper.rev_telephone(v[0] if len(v) else u'')
      elif k == 'amenity':
        item[k] = helper.clean_lf(v, u', ')
      elif k == 'page_breadcrumbs':
        vlen = len(v)
        item[k] = v[:3] if vlen > 3 else v
      elif k.startswith('page_body'):
        pass
      else:
        item[k] = helper.clean_lf(v)
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
    row.append(u'%s, %s, %s %s%s' % (item['address_street'], item['address_locality'], item['address_region'], item['address_zip'], item['address_area_name']))
    row.append(u'%s, %s, %s %s%s' % (item['address_street_ja'], item['address_locality_ja'], item['address_region_ja'], item['address_zip_ja'], item['address_area_name']))
    row.append(u'%s, %s, %s %s%s' % (item['address_street_es'], item['address_locality_es'], item['address_region_es'], item['address_zip_es'], item['address_area_name']))
    row.append(u'%s, %s, %s %s%s' % (item['address_street_zh'], item['address_locality_zh'], item['address_region_zh'], item['address_zip_zh'], item['address_area_name']))
    row.append(item['phone'])
    row.append(item['amenity'])
    row.append(item['amenity_ja'])
    row.append(item['amenity_es'])
    row.append(item['amenity_zh'])
    row.append(u'%s%%' % item['popularity'])
    CsvWriter.write_to_csv(CSV_OUTPUT_FILE, row)
