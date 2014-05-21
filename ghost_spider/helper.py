# -*- coding: utf-8 -*-
import urllib, os

# selectors for pages by language
SEL_JAPANESE_PAGE = '/html/head/link[@hreflang="ja"]/@href'
SEL_SPANISH_PAGE = '/html/head/link[@hreflang="es"]/@href'
SEL_CHINESE_PAGE = '/html/head/link[@hreflang="zh-Hans"]/@href'

# Selector for name of Place
SEL_HOTEL_NAME = '//h1[@id="HEADING" and @rel="v:name"]/text()'

# Selector for address
SEL_AREA_NAME = '//div[@id="HEADING_GROUP"]/div/address/text()'
SEL_AREA_STREET = '//div[@id="HEADING_GROUP"]/div/address/span[@rel="v:address"]/span[@class="format_address"]/span[@property="v:street-address"]/text()'
SEL_AREA_LOCALITY = '//div[@id="HEADING_GROUP"]/div/address/span[@rel="v:address"]/span[@class="format_address"]/span/span[@property="v:locality"]/text()'
SEL_AREA_REGION = '//div[@id="HEADING_GROUP"]/div/address/span[@rel="v:address"]/span[@class="format_address"]/span/span[@property="v:region"]/text()'
SEL_AREA_ZIP = '//div[@id="HEADING_GROUP"]/div/address/span[@rel="v:address"]/span[@class="format_address"]/span/span[@property="v:postal-code"]/text()'


# Selector for amenities
SEL_AMENITIES = '//div[contains(@class, "amenitiesRDV1")]/div[contains(@class,"amenity")]/text()'

# Selector for phone number
SEL_PHONE_NUMBER = '//div[@id="HEADING_GROUP"]/div[contains(@class, "wrap")]/div[contains(@class, "blDetails")]/div[3]/div[@class="fl"]/script/text()'

# Selector for URL


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
  #print a2_or_1

  telephone = {}
  commands = a2_or_1.split('\n')
  #print commands
  for command in commands:
    #print command
    letter = command.split(u'=')
    if len(letter) >= 2:
      if u'+' in letter[0]:
        telephone['%s' % letter[0][0]] += letter[1].strip(u'\'')
      else:
        telephone['%s' % letter[0]] = letter[1].strip(u'\'')
  #print telephone
  try:
    phone_number = telephone['a'] + telephone['c'] + telephone['b']
  except:
    phone_number = u'_'
  return phone_number


def get_website(url):
  """read any url and return response."""
  try:
    fp = urllib.urlopen(url)
    return fp
  except:
    return None


def debug_screen(value):
  """print any value on screen."""
  print ".*." * 50
  print value
  print ".*." * 50