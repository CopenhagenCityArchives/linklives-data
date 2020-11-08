FROM python:3.7

COPY ./ /indexer
WORKDIR /indexer
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
ENTRYPOINT python index.py setup --es-host ll-es:9200 && python index.py index --es-host ll-es:9200 --csv-dir /indexer/csv-index-test