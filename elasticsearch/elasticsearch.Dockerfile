FROM docker.elastic.co/elasticsearch/elasticsearch:7.9.3

ADD elasticsearch.yml /usr/share/elasticsearch/config

RUN sudo bin/elasticsearch-plugin install repository-s3