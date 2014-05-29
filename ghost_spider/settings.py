# -*- coding: utf-8 -*-

# Scrapy settings for ghost_spider project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'ghost_spider'

SPIDER_MODULES = ['ghost_spider.spiders']
NEWSPIDER_MODULE = 'ghost_spider.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36'
DOWNLOAD_DELAY = 1
COOKIES_ENABLED = False


ITEM_PIPELINES = {
    'ghost_spider.pipelines.GhostSpiderPipeline': 0
}

ELASTICSEARCH_SERVER = ('192.168.57.101:9200', )

DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en',
}

REQUEST_HEADERS = {
    'en': DEFAULT_REQUEST_HEADERS,
    'ja': {
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'ja'
    },
    'es': {
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'es'
    },
    'fr': {
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'fr'
    },
    'zh': {
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'zh-CN;q=0.8,zh;q=0.6'
    }
}


def setup_elastic_connection():
  import slimes
  es_requester = slimes.Requester(ELASTICSEARCH_SERVER)
  return es_requester
es = setup_elastic_connection()

CSV_OUTPUT_FILE = "output/data.csv"
LOG_OUTPUT_FILE = "output/error-log.txt"
LOG_OUTPUT_FILE_INFO = "output/info-log.txt"

LOG_ENABLED = True
LOG_FILE = LOG_OUTPUT_FILE_INFO
LOG_LEVEL = 'INFO'
LOG_STDOUT = False
HTTPERROR_ALLOW_ALL = True
