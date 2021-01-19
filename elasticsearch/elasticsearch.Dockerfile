FROM docker.elastic.co/elasticsearch/elasticsearch:7.9.3

ADD elasticsearch.yml /usr/share/elasticsearch/config

RUN bin/elasticsearch-plugin install -b repository-s3