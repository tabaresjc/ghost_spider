To have launchd start elasticsearch at login:
    ln -sfv /usr/local/opt/elasticsearch/*.plist ~/Library/LaunchAgents
Then to load elasticsearch now:
    launchctl load ~/Library/LaunchAgents/homebrew.mxcl.elasticsearch.plist
Or, if you don't want/need launchctl, you can just run:
    elasticsearch --config=/usr/local/opt/elasticsearch/config/elasticsearch.yml
==> Summary
🍺  /usr/local/Cellar/elasticsearch/1.1.1: 31 files, 21M, built in 41 seconds
elasticsearch: stable 1.1.1, HEAD
http://www.elasticsearch.org
/usr/local/Cellar/elasticsearch/1.1.1 (31 files, 21M) *
  Built from source
From: https://github.com/Homebrew/homebrew/commits/master/Library/Formula/elasticsearch.rb
==> Caveats
Data:    /usr/local/var/elasticsearch/elasticsearch_jctt/
Logs:    /usr/local/var/log/elasticsearch/elasticsearch_jctt.log
Plugins: /usr/local/var/lib/elasticsearch/plugins/

To have launchd start elasticsearch at login:
    ln -sfv /usr/local/opt/elasticsearch/*.plist ~/Library/LaunchAgents
Then to load elasticsearch now:
    launchctl load ~/Library/LaunchAgents/homebrew.mxcl.elasticsearch.plist
Or, if you don't want/need launchctl, you can just run:
    elasticsearch --config=/usr/local/opt/elasticsearch/config/elasticsearch.yml