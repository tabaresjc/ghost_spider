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
USER_AGENT = 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.16 Safari/537.36'
#DOWNLOAD_DELAY = 2
COOKIES_ENABLED = False


ITEM_PIPELINES = {
    'ghost_spider.pipelines.GhostSpiderPipeline': 0
}

ELASTICSEARCH_SERVER = ('192.168.56.101:9200', )
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en',
}


def setup_elastic_connection():
  import slimes
  es_requester = slimes.Requester(ELASTICSEARCH_SERVER)
  return es_requester
es = setup_elastic_connection()
