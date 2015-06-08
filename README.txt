- ElasticSearch 1.x

  [Installation]
    JDK MUST be oracle JDK 1.7 others JDK have problems
    http://exploringelasticsearch.com/book/elasticsearch-at-scale-interviews/interview-with-the-github-elasticsearch-team.html
    http://www.elasticsearch.org/

      After installing elasticSearch:
      - Copy the dictionary & synonyms
        sudo cp /schema/luxeysdict_ja.txt /etc/elasticsearch/
        sudo cp /schema/luxeyssyn_ja.txt /etc/elasticsearch/

      - Install plugins (from /usr/share/elasticsearch/ [default directory])
          #used for analysing the Japanese data (must install)
          #
          # /!\ BE CAREFUL with the version check the website to be sure that the plugin work with your elastic
          #
          bin/plugin -install elasticsearch/elasticsearch-analysis-kuromoji/2.0.0
          copy the change from the schema/elasticsearch.yml to /etc/elaticsearch/elasticsearch.yml
          Restart elastic "/etc/init.d/elasticsearch restart"
          #run the script to setup index and type
          /scripts/setup.sh

- Python Libraries: Install the required python libraries listed in requeriments.txt

- How to Use
  [Launch crawler]
    To launch crawler for salon (ghost_pider/spiders/salon_spider.py), run following command
    $ scrapy crawl salon

  [Shell]
    Use this to fetch and analyze web pages, it is very useful when you need to test the xpath selectors
    $ scrapy shell '/path/to/file/or/url'
