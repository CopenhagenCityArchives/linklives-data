import sqlite3
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError as ESNotFoundError
from neo4j import GraphDatabase, basic_auth
from math import ceil
import requests

def index_graph_all_nodes(sqlite_db, neo4j):
    with sqlite3.connect(sqlite_db) as sqlite:
        sqlite.row_factory = sqlite3.Row
        c = sqlite.cursor()
        c.execute('SELECT COUNT(*) as c FROM census_1860')
        count = c.fetchone()['c']
        for i in range(ceil(count/10000)):
            print(f"census_1860: {i*10000}/{count}")
            for row in c.execute(f'SELECT * FROM census_1860 LIMIT {i*10000},10000'):
                props = dict(row)
                props['source_id'] = 0
                neo4j.run('CREATE (pa:PersonAppearance $props)', props=dict(row))
        c.execute('SELECT COUNT(*) as c FROM census_1850')
        count = c.fetchone()['c']
        for i in range(ceil(count/10000)):
            print(f"census_1850: {i*10000}/{count}")
            for row in c.execute(f'SELECT * FROM census_1850 LIMIT {i*10000},10000'):
                props = dict(row)
                props['source_id'] = 1
                neo4j.run('CREATE (pa:PersonAppearance $props)', props=dict(row))
        c.execute('SELECT COUNT(*) as c FROM census_1845')
        count = c.fetchone()['c']
        for i in range(ceil(count/10000)):
            print(f"census_1845: {i*10000}/{count}")
            for row in c.execute(f'SELECT * FROM census_1845 LIMIT {i*10000},10000'):
                props = dict(row)
                props['source_id'] = 2
                neo4j.run('CREATE (pa:PersonAppearance $props)', props=dict(row))


def index_graph_linked_nodes(sqlite_db, neo4j, table_name, source_id):
    with sqlite3.connect(sqlite_db) as sqlite:
        sqlite.row_factory = sqlite3.Row
        c = sqlite.cursor()

        props = list(map(dict, c.execute(f'SELECT DISTINCT C.* FROM {table_name} C INNER JOIN Links L ON L.pa_id = C.pa_id AND L.source_id = {source_id}')))
        for prop in props:
            prop['source_id'] = source_id
        neo4j.run('UNWIND $props AS prop CREATE (pa:PersonAppearance) SET pa = prop', props=props)

def get_links(sqlite_db):
    with sqlite3.connect(sqlite_db) as sqlite:
        sqlite.row_factory = sqlite3.Row
        c = sqlite.cursor()
        for row in c.execute(f"""
            SELECT
                L.link_id,
                L.score,
                LC.life_course_id,
                group_concat(L.pa_id, ',') as pa_ids,
                group_concat(L.source_id, ',') as source_ids
            FROM Links L
            LEFT JOIN Life_courses LC ON LC.link_id = L.link_id
            GROUP BY L.link_id"""):
            link = {}
            link['pa_id_a'], link['pa_id_b'] = row['pa_ids'].split(',')
            link['source_id_a'], link['source_id_b'] = row['source_ids'].split(',')
            link['life_course_id'] = row['life_course_id']
            link['score'] = row['score']
            link['link_id'] = row['link_id']
            yield link

def index_graph_links(sqlite_db, neo4j):
    for l in get_links(sqlite_db):
        neo4j.run(f'''
        MATCH (a:PersonAppearance {{ pa_id: {l["pa_id_a"]}, source_id: {l["source_id_a"]} }})
        MATCH (b:PersonAppearance {{ pa_id: {l["pa_id_b"]}, source_id: {l["source_id_b"]} }})
        CREATE (a)-[ll:LifeLink{{ link_id: {l["link_id"]}, life_course_id: {l["life_course_id"]}, score: {l["score"]}}}]->(b)
        ''').consume()


def index_nested(sqlite_db, es):
    with open('lifecourses_full_sorted.sql', 'r', encoding='utf-8') as sqlfile:
        query = sqlfile.read()

    with sqlite3.connect(sqlite_db) as sqlite:
        sqlite.row_factory = sqlite3.Row
        c = sqlite.cursor()
        last_life_course_id = None
        last_link_id = None
        life_course = {}
        link = []
        for row in c.execute(query):
            index_nested_pa(row)
            if life_course and last_life_course_id != row['life_course_id']:
                index_nested_lifecourse(life_course)
                life_course = {}
            if link and last_link_id != row['link_id']:
                index_nested_link(link)
                link = []
            life_course[row['pa_id']] = row
            link.append(row)
            last_life_course_id = row['life_course_id']
            last_link_id = row['link_id']

def index_nested_pa(pa):
    doc = {
        "personal_appearance": {key: pa[key] for key in dict(pa) if pa[key] != ""}
    }
    es.index(index="pas", id=f"{pa['source_id']}-{pa['pa_id']}", body=doc)

def index_nested_link(link):
    doc = {
        "personal_appearance": list(map(lambda row: {key: row[key] for key in dict(row) if row[key] != ""}, link))
    }
    es.index(index="links", id=link[0]['link_id'], body=doc)

def index_nested_lifecourse(life_course):
    doc = {
        "personal_appearance": list(map(lambda row: {key: row[key] for key in dict(row) if row[key] != ""}, life_course.values()))
    }
    es.index(index='lifecourses', id=list(life_course.values())[0]['life_course_id'], body=doc)

def index_flat(sqlite_db, es):
    with open('lifecourses_full_sorted.sql', 'r', encoding='utf-8') as sqlfile:
        query = sqlfile.read()

    with sqlite3.connect(sqlite_db) as sqlite:
        sqlite.row_factory = sqlite3.Row
        c = sqlite.cursor()
        for row in c.execute(query):
            es.index(index='link-lives', id=f"{row['life_course_id']}-{row['pa_id']}-{row['source_id']}", body=dict(row))

def es_mapping_pa_props():
    return {
        "firstnames": {"type": "text"},
        "surnames": {"type": "text"},
        "familyname": {"type": "text"},
        "firstnames_std": {"type": "keyword"},
        "surnames_std": {"type": "keyword"},
        "maidenname_std": {"type": "keyword"},
        "familyname_std": {"type": "keyword"},
        "sex": {"type": "keyword"},
        "birthplace": {"type": "text"},
        "birthdate": {"type": "date"},
        "deathdate": {"type": "date"},
        "county": {"type": "text"},
        "district": {"type": "text"},
        "parish": {"type": "text"},
        "county_std": {"type": "keyword"},
        "district_std": {"type": "keyword"},
        "parish_std": {"type": "keyword"}
    }

def es_mapping_pa():
    return {
        "type": "nested",
        "properties": {
            "link_id": {"type": "integer"},
            "lifecourse_id": {"type": "integer"},

            "firstnames": {"type": "text"},
            "surnames": {"type": "text"},
            "familyname": {"type": "text"},
            "firstnames_std": {"type": "keyword"},
            "surnames_std": {"type": "keyword"},
            "maidenname_std": {"type": "keyword"},
            "familyname_std": {"type": "keyword"},
            "sex": {"type": "keyword"},
            "birthplace": {"type": "text"},
            "birthdate": {"type": "date"},
            "deathdate": {"type": "date"},
            "county": {"type": "text"},
            "district": {"type": "text"},
            "parish": {"type": "text"},
            "county_std": {"type": "keyword"},
            "district_std": {"type": "keyword"},
            "parish_std": {"type": "keyword"},
        }
    }

def es_mappings_index_lifecourses():
    return {
        "dynamic": False,
        "properties": {
            "id": {"type": "integer"},
            "personal_appearance": es_mapping_pa()
        }
    }

def es_mappings_index_links():
    return {
        "dynamic": False,
        "properties": {
            "id": {"type": "integer"},
            "personal_appearance": es_mapping_pa(),
            "method": {"type": "keyword"},
            "score": {"type": "float"}
        }
    }

def es_mappings_index_pas():
    return {
        "dynamic": False,
        "properties": {
            "id": {"type": "integer"},
            "personal_appearance": es_mapping_pa()
        }
    }


if __name__ == "__main__":
    import sys
    if sys.argv[1] == 'es':
        es = Elasticsearch(hosts=["localhost:9200", "localhost:9300"])
        if len(sys.argv) == 3 and sys.argv[2] == "setup":
            print("deleting indices")
            try:
                es.indices.delete("links,lifecourses,pas")
            except:
                pass

            print("setting up indices")
            print(" => creating links index")
            es.indices.create('links')
            print(" => putting links mapping")
            es.indices.put_mapping(index='links', body=es_mappings_index_links())

            print(" => creating lifecourses index")
            es.indices.create('lifecourses')   
            print(" => putting lifecourse mapping")
            es.indices.put_mapping(index='lifecourses', body=es_mappings_index_lifecourses())         

            print(" => creating pas index")
            es.indices.create('pas')
            print(" => putting pas mapping")
            es.indices.put_mapping(index='pas', body=es_mappings_index_pas())
        else:
            index_nested('LL.db', es)
    elif sys.argv[1] == 'graph':
        driver = GraphDatabase.driver('bolt://localhost', auth=basic_auth('neo4j', '123456'))
        session = driver.session()
        if sys.argv[2] == "nodes":
            print("indexing nodes")
            print('census 1860')
            #index_graph_linked_nodes('LL.db', session, "census_1860", 0)
            print('census 1850')
            #index_graph_linked_nodes('LL.db', session, "census_1850", 1)
            print('census 1845')
            index_graph_linked_nodes('LL.db', session, "census_1845", 2)
        elif sys.argv[2] == "links":
            print("indexing links")
            index_graph_links('LL.db', session)