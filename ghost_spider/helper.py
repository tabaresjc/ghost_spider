# -*- coding: utf-8 -*-

import urllib
import re


class SalonSelectors(object):
  """List of selectors for Salons."""
  LIST_SALONS = '//div[contains(@class, "uWrap")]/div[contains(@class, "LSaj")]/div[contains(@class, "item")]//h3[contains(@class, "ttl")]/a/@href'
  NEXT_URL = '//link[@rel="next"]/@href'
  CANONICAL_URL = '//link[@rel="canonical"]/@href'

  NAME = '//div[contains(@class, "title")]//p[contains(@class, "poiTtl")]/a/text()'
  NAME_KATA = '//div[contains(@class, "title")]/div[contains(@class, "ruby")]/text()'
  ADDRESS = '//div[contains(@class, "access")]/p[contains(@class, "address")]/text()'
  ROUTES = '//div[contains(@class, "access")]/p[contains(@class, "route")]/text()'

  SEL_META = '//head/meta'
  SEL_INFO = '//div[@id="outline"]/ul'
  SEL_START = '//div[@id="outline"]/ul'
  SEL_TITLE = '//div[contains(@class, "title")]'
  SEL_BREADCRUMBS = '//div[@id="sHeader"]/div[contains(@class, "link")]/p[contains(@class, "fl")]/a/text()'

  GENERAL_INFO_TABLE = u'//ul[contains(@class, "detailInfo")]//dt[contains(., "%s")]/following-sibling::dd/text()'
  GENERAL_INFO_TABLE_URL = u'//ul[contains(@class, "detailInfo")]//dt[contains(., "%s")]/following-sibling::dd/a/text()'
  GENERAL_INFO_TABLE_CARDS = u'//ul[contains(@class, "detailInfo")]//dt[contains(., "%s")]/following-sibling::dd/span[contains(@class, "logoIcon")]/text()'

  @classmethod
  def get_prefecture_area(cls, sel):
    """extract the most relevant info from the page."""
    data = sel.xpath(cls.SEL_BREADCRUMBS).extract()
    if not data:
      return [], u'', u''
    breadcrumbs = data[1:]
    prefecture = breadcrumbs[0]

    for word in [u'県', u'府', u'都']:
      if prefecture[-1] == word:
        prefecture = prefecture.replace(word, u'')
        break

    area = u''
    for place in breadcrumbs[1:]:
      if u'・' in place:
        continue
      area = place
      break

    if not area and len(breadcrumbs) > 1:
      area = breadcrumbs[1]

    return prefecture, area

  @classmethod
  def get_body(cls, sel):
    """extract the most relevant info from the page."""
    meta = sel.xpath(cls.SEL_META).extract()
    breadcrumbs = sel.xpath(cls.SEL_BREADCRUMBS).extract()
    info = sel.xpath(cls.SEL_INFO).extract()
    title = sel.xpath(cls.SEL_TITLE).extract()
    if breadcrumbs:
      breadcrumbs = breadcrumbs[1:]
    body = {
      'meta': u''.join(meta),
      'breadcrumbs': breadcrumbs,
      'info': info[0] if len(info) else u'',
      'title': title[0] if len(title) else u'',
    }
    return body

  @classmethod
  def get_routes(cls, sel):
    """extract the most relevant info from the page."""
    raw_routes = sel.xpath(cls.ROUTES).extract()
    routes = [_.strip().replace(u'（', u'').replace(u'）', u'') for _ in raw_routes]
    return [route for route in routes if route]

  @classmethod
  def get_phone(cls, sel):
    raw_data = sel.xpath(cls.GENERAL_INFO_TABLE % u'電話番号').extract()
    return u''.join(raw_data).strip()

  @classmethod
  def get_working_hours(cls, sel):
    raw_data = sel.xpath(cls.GENERAL_INFO_TABLE % u'営業時間').extract()
    raw_data = [_.strip() for _ in raw_data]
    return u'\n'.join(raw_data)

  @classmethod
  def get_holidays(cls, sel):
    raw_data = sel.xpath(cls.GENERAL_INFO_TABLE % u'定休日').extract()
    raw_data = [_.strip() for _ in raw_data]
    return u'\n'.join(raw_data)

  @classmethod
  def get_shop_url(cls, sel):
    raw_data = sel.xpath(cls.GENERAL_INFO_TABLE_URL % u'HP').extract()
    raw_data = [_.strip() for _ in raw_data]
    return u'\n'.join(raw_data)

  @classmethod
  def get_credit_cards(cls, sel):
    raw_data = sel.xpath(cls.GENERAL_INFO_TABLE_CARDS % u'利用可能カード').extract()
    raw_data = [_.strip() for _ in raw_data]
    ccards = []
    for card in raw_data:
      if card == u'VISA':
        ccards.append(u'VISA')
      elif card == u'MasterCard':
        ccards.append(u'MASTER')
      elif card == u'JCB':
        ccards.append(u'JCB')
      elif card == u'AmericanExpress' or card == 'American Express':
        ccards.append(u'AMERICAN EXPRESS')
      elif card == u'ダイナース' or card == u'DINERS':
        ccards.append(u'DINERS')
      elif card == u'Discover':
        ccards.append(u'DISCOVER')
    comment = u''
    if ccards:
      comment = u'利用可'
    else:
      # check if this shop can accept cards
      features = u''.join(sel.xpath(cls.GENERAL_INFO_TABLE % u'特徴').extract()).strip()
      if u'カード利用' in features:
        comment = u'利用可'
      else:
        comments = u''.join(sel.xpath(cls.GENERAL_INFO_TABLE % u'クレジットカードコメント').extract()).strip().lower()
        avalaible = [u'visa', u'mastercard', u'jcb', u'american express', u'ダイナース', u'その他', 'diner', 'discover']
        for ava in avalaible:
          if ava in comments:
            comment = u'利用可'
            break
    return comment, ccards

  @classmethod
  def get_seats(cls, sel):
    raw_data = sel.xpath(cls.GENERAL_INFO_TABLE % u'総席数').extract()
    return u''.join(raw_data).strip()

  @classmethod
  def get_stylist(cls, sel):
    raw_data = sel.xpath(cls.GENERAL_INFO_TABLE % u'スタイリスト人数').extract()
    return u''.join(raw_data).strip()

  @classmethod
  def get_parking(cls, sel):
    raw_data = sel.xpath(cls.GENERAL_INFO_TABLE % u'駐車場').extract()
    raw_data = [_.strip() for _ in raw_data]
    return u'\n'.join(raw_data)

  @classmethod
  def get_cut_price(cls, sel):
    raw_data = sel.xpath(cls.GENERAL_INFO_TABLE % u'カット料金').re(r'\d+')
    if len(raw_data) > 1:
      raw_data = sel.xpath(cls.GENERAL_INFO_TABLE % u'カット料金').extract()
      raw_data = u''.join(raw_data).strip()
    elif len(raw_data) == 1:
      raw_data = u''.join(raw_data).strip()
      try:
        raw_data = int(raw_data)
      except:
        pass
    else:
      raw_data = u''
    return raw_data


# selector for country, prefectures and areas
SEL_LIST_PLACES = '//div[@id="BODYCON"]/table[1]/tr/td/a'
SEL_ALLOW_PLACES = '//div[@id="download_countries"]/text()'

SEL_LIST_MORE = '//div[@id="BODYCON"]/div[contains(text(),"More Accommodations")]/a'
place_sel_name = re.compile(r'Lodging in\s*(.*)<', re.DOTALL)
place_sel_link = re.compile(r'href="(.*)"', re.DOTALL)

SEL_LIST_PLACES_LAST = '//div[@id="BODYCON"]/table[1]/tr/td/div/a'
place_sel_name_last = re.compile(r'>(.*)<', re.DOTALL)
place_sel_link_last = re.compile(r'href="(.*)"', re.DOTALL)
place_sel_place_type = re.compile(r'<span class="placeTypeText">(.*)</span>', re.DOTALL)

# selectors for pages by language
SEL_JAPANESE_PAGE = '/html/head/link[@hreflang="ja"]/@href'
SEL_SPANISH_PAGE = '/html/head/link[@hreflang="es"]/@href'
SEL_FRENCH_PAGE = '/html/head/link[@hreflang="fr"]/@href'
SEL_CHINESE_PAGE = '/html/head/link[@hreflang="zh-Hans"]/@href'

# Selector for name of Place
SEL_HOTEL_NAME = '//h1[@id="HEADING" and (@rel="v:name" or @property="v:name")]/text()'

# Selector for address
SEL_AREA_NAME = '//div[@id="HEADING_GROUP"]/div/address/text()'
SEL_AREA_STREET = '//div[@id="HEADING_GROUP"]/div/address/span/span/span[@property="v:street-address"]/text()'
SEL_AREA_LOCALITY = '//div[@id="HEADING_GROUP"]/div/address/span/span/span/span[@property="v:locality"]/text()'
SEL_AREA_REGION = '//div[@id="HEADING_GROUP"]/div/address/span/span/span/span[@property="v:region"]/text()'
SEL_AREA_ZIP = '//div[@id="HEADING_GROUP"]/div/address/span/span/span/span[@property="v:postal-code"]/text()'


# Selector for amenities
SEL_AMENITIES = '//div[contains(@class, "amenitiesRDV1")]/div[contains(@class,"amenity")]/text()'

# Selector for phone number
SEL_PHONE_NUMBER = '//div[@id="HEADING_GROUP"]/div[contains(@class, "wrap")]'
SEL_RE_PHONE_NUMBER = re.compile(u'escramble.+?document', re.DOTALL)
# Selector for URL
SEL_URL = '//div[@id="HEADING_GROUP"]/div[contains(@class, "wrap")]'
SEL_RE_URL = re.escape(u'/ShowUrl?&excludeFromVS') + '.*?(?=")'

# Selector for rating
SEL_RATING = '//div[@id="HEADING_GROUP"]/div/address/span/span[contains(@class,"rate")]/img/@alt'
SEL_PERCENT = '//div[@class="recommendedPercent"]/span[@class="percent"]/text()'

# Selector for breadcrumbs
SEL_BREADCRUMBS = '//ul[@class="breadcrumbs"]/li/a/span/text()'

# Selector for body (just select few parts of the page !! dont' be so greedy!)
SEL_HEADING = '//div[@id="HEADING_GROUP"]'
SEL_META = '//head/meta'
SEL_AMENITY_DIV = '//div[@id="AMENITIES_OVERLAY_HIDDEN"]'
SEL_LOCATION_CONTENT = '//div[@id="HR_HACKATHON_CONTENT"]/div/div[contains(@class,"locationContent")]'

CLEAN_STATE = re.compile(r'(.*)\s\(', re.DOTALL)

FIND_HOTEL_LINK = re.compile(r'(?i)hotel', re.DOTALL)


def get_body(sel):
  """extract the most relevant info from the page."""
  body = []
  body.append(sel.xpath(SEL_HEADING).extract())
  body.append(sel.xpath(SEL_META).extract())
  body.append(sel.xpath(SEL_AMENITY_DIV).extract())
  body.append(sel.xpath(SEL_LOCATION_CONTENT).extract())
  body = [b[0] for b in body if b and len(b)]
  return body


def clean_lf(value, sep=u''):
  if isinstance(value, dict):
    return value
  elif isinstance(value, (list, tuple)):
    return sep.join(value).replace('\n', '')
  else:
    return value.replace('\n', '')


def rev_telephone(scrambled):
  a2_or_1 = scrambled.split('var')
  a2_or_1 = u''.join([x for x in a2_or_1 if u'E' not in x])
  # print a2_or_1

  telephone = {}
  commands = a2_or_1.split('\n')
  # print commands
  for command in commands:
    # print command
    letter = command.split(u'=')
    if len(letter) >= 2:
      if u'+' in letter[0]:
        telephone['%s' % letter[0][0]] += letter[1].strip(u'\'')
      else:
        telephone['%s' % letter[0]] = letter[1].strip(u'\'')
  # print telephone
  try:
    phone_number = telephone['a'] + telephone['c'] + telephone['b']
  except:
    phone_number = u''
  return phone_number


def get_weburl(url):
  try:
    fp = urllib.urlopen(url)
  except:
    return u''
  return fp.geturl()


def debug_screen(value):
  """print any value on screen."""
  print ".*." * 50
  print value
  print ".*." * 50
