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
#USER_AGENT = 'ghost_spider (+http://www.yourdomain.com)'
