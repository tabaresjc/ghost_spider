# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from ghost_spider import helper
from elastic import LocationHs


class GhostSpiderPipeline(object):

  """Process & format data after being scrapped from page."""

  def process_item(self, item, spider):
    for k, v in item.iteritems():
      if k == 'phone':
        if v and len(v):
          v = helper.SEL_RE_PHONE_NUMBER.findall(v[0])
        item[k] = helper.rev_telephone(v[0] if len(v) else u'')
      elif k.startswith('amenity'):
        item[k] = helper.clean_lf(v, u', ')
      elif k == 'page_breadcrumbs':
        if v and len(v):
          item[k] = v[:len(v) - 1] if v else []
        else:
          item[k] = []
      elif k.startswith('page_body'):
        pass
      else:
        item[k] = helper.clean_lf(v)

    if not LocationHs.check_by_name(item['name']):
      LocationHs.save(self.save_item_to_es(item))
      LocationHs.refresh()
    return item

  def save_item_to_es(self, item):
    item_es = {}
    item_es['name_low'] = item['name'].lower().stripe()
    item_es['rating'] = float(item['rating'] or 0)
    item_es['popularity'] = float(item['popularity'] or 0)
    item_es['page_url'] = item['page_url'].lower()
    item_es['page_breadcrumbs'] = item['page_breadcrumbs']
    item_es['phone'] = item['phone']
    item_es['area1'] = item['page_breadcrumbs'][0].stripe() if len(item['page_breadcrumbs']) > 0 else u''
    item_es['area2'] = item['page_breadcrumbs'][1].stripe() if len(item['page_breadcrumbs']) > 1 else u''
    state = helper.CLEAN_STATE.findall(item_es['area2'])
    if state:
      item_es['area2'] = state[0].strip()
    item_es['area3'] = item['page_breadcrumbs'][2].stripe() if len(item['page_breadcrumbs']) > 2 else u''
    item_es['area4'] = item['page_breadcrumbs'][3].stripe() if len(item['page_breadcrumbs']) > 3 else u''
    item_es['area5'] = item['page_breadcrumbs'][4].stripe() if len(item['page_breadcrumbs']) > 4 else u''
    item_es['region'] = item['address_region'].stripe()
    place = []
    for lang in ['en', 'ja']:
      p = {
        'lang': lang
      }
      for s in ['name', 'address_area_name', 'address_locality', 'address_street', 'address_region', 'address_zip', 'amenity', 'page_body']:
        nk = '%s_%s' % (s, lang) if lang != 'en' else s
        p.update({s: item[nk]})
      place.append(p)
    item_es['place'] = place
    return item_es
