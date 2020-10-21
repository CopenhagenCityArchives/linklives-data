FROM docker.elastic.co/elasticsearch/elasticsearch:7.5.2

ADD memory.options /usr/share/elasticsearch/config/jvm.options.d
ADD elasticsearch.yml /usr/share/elasticsearch/config