FROM python:3.7

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY index.py index.py
ENTRYPOINT python /indexer/index.py setup && python /indexer/index.py index /indexer-data/linklives-data-latest.db