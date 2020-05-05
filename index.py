import sqlite3
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError as ESNotFoundError
from math import ceil
import requests


def index(sqlite_db, es):
    print(" => reading sources")
    sources = list(read_sources(sqlite_db))

    print(" => creating source readers")
    readers = {source['source_id']: {'reader': read_source_chunks(sqlite_db, source), 'pointer': 0, 'data': None} for source in sources}

    print(" => indexing data")
    life_course_count = 0
    pa_count = 0
    link_count = 0
    while True:
        life_course_id = get_life_course_id(readers)

        # this means no more data
        if life_course_id is None:
            break
        
        life_course = generate_life_course(readers, life_course_id)
        pa_count += len(set([(pa['pa_id'], pa['source_id']) for pa in life_course]))
        
        # generate the links of the life course
        links = {}
        for pa in life_course:
            if pa['link_id'] not in links:
                links[pa['link_id']] = [pa]
            else:
                links[pa['link_id']].append(pa)

        for link in links.values():
            index_link(link)
            link_count += 1

        index_life_course(life_course)
        life_course_count += 1

        print(f" => personal appearances: {pa_count}, links: {link_count}, life courses: {life_course_count}", end="\r")


def get_life_course_id(readers):
    life_course_id = None
    
    for source_id in readers:
        data = reader_peek(readers, source_id)

        if data is not None and (life_course_id is None or life_course_id > data['life_course_id']):
            life_course_id = data['life_course_id']
    
    return life_course_id


def reader_peek(readers, source_id):
    reader = readers[source_id]

    if reader['data'] is None or reader['pointer'] == len(reader['data']):
            reader['data'] = next(reader['reader'])
            reader['pointer'] = 0
    
    if len(reader['data']) > 0:
        return reader['data'][reader['pointer']]


def generate_life_course(readers, life_course_id):
    life_course = []

    for source_id in readers:
        data = reader_peek(readers, source_id)
        if data['life_course_id'] == life_course_id:
            life_course.append(data)
            readers[source_id]['pointer'] += 1

            # index the pa when added to life course, because it wont be read anymore
            index_pa(data)
        else:
            # go to next reader to prevent unneccesary double check
            continue

        # handle pas being part of two links by getting the next too
        data = reader_peek(readers, source_id)
        if data['life_course_id'] == life_course_id:
            life_course.append(data)
            readers[source_id]['pointer'] += 1

    return life_course


def read_sources(sqlite_db):
    with sqlite3.connect(sqlite_db) as sqlite:
        sqlite.row_factory = sqlite3.Row
        c = sqlite.cursor()
        for row in c.execute("SELECT * FROM Sources"):
            yield dict(row)

def read_source_chunks(sqlite_db, source):
    with sqlite3.connect(sqlite_db) as sqlite:
        sqlite.row_factory = sqlite3.Row
        c = sqlite.cursor()
        count = list(c.execute(f'SELECT count(*) as count FROM {source["table_name"]}'))[0]['count']
        offset = 0
        while offset < count:
            yield list(c.execute(f"SELECT * FROM {source['table_name']} p JOIN Links l ON l.source_id = {source['source_id']} AND l.pa_id = p.pa_id JOIN Life_courses lc ON lc.link_id = l.link_id ORDER BY lc.life_course_id, l.link_id, p.pa_id ASC LIMIT {offset},100"))
            offset += 100

def index_pa(pa):
    doc = {
        "personal_appearance": {key: pa[key] for key in dict(pa) if pa[key] != ""}
    }
    es.index(index="pas", id=f"{pa['source_id']}-{pa['pa_id']}", body=doc)

def index_link(link):
    doc = {
        "personal_appearance": list(map(lambda row: {key: row[key] for key in dict(row) if row[key] != ""}, link))
    }
    es.index(index="links", id=link[0]['link_id'], body=doc)

def index_life_course(life_course):
    doc = {
        "personal_appearance": list(map(lambda row: {key: row[key] for key in dict(row) if row[key] != ""}, life_course))
    }
    if len(life_course) == 1:
        raise Exception(life_course)
    es.index(index='lifecourses', id=list(life_course)[0]['life_course_id'], body=doc)

def mapping_pa():
    return {
        "type": "nested",
        "properties": {
            "link_id": {"type": "integer"},
            "lifecourse_id": {"type": "integer"},

            "pa_id": {"type": "integer"}, 
            "løbenr_i_indtastning": {"type": "float"}, 
            "Stednavn": {"type": "text"}, 
            "name": {"type": "text"}, 
            "age": {"type": "float"}, 
            "Erhverv": {"type": "text"}, 
            "Stilling_i_husstanden": {"type": "text"}, 
            "birth_place": {"type": "text"}, 
            "gender": {"type": "text"}, 
            "Sogn": {"type": "text"}, 
            "Amt": {"type": "text"}, 
            "Herred": {"type": "text"}, 
            "gender_clean": {"type": "text"}, 
            "name_clean": {"type": "keyword"}, 
            "age_clean": {"type": "float"}, 
            "hh_id": {"type": "integer"}, 
            "hh_pos_std": {"type": "text"}, 
            "is_husband": {"type": "integer"}, 
            "has_husband": {"type": "integer"}, 
            "name_std": {"type": "text"}, 
            "maiden_family_names": {"type": "text"}, 
            "maiden_patronyms": {"type": "text"}, 
            "first_names": {"type": "text"}, 
            "patronyms": {"type": "text"}, 
            "family_names": {"type": "text"}, 
            "uncat_names": {"type": "text"}, 
            "husband_first_names": {"type": "text"}, 
            "husband_name_match": {"type": "integer"}, 
            "true_patronym": {"type": "text"}, 
            "all_possible_patronyms": {"type": "text"}, 
            "all_possible_family_names": {"type": "text"}, 
            "b_place_cl": {"type": "keyword"}, 
            "other_cl": {"type": "keyword"}, 
            "parish_cl": {"type": "keyword"}, 
            "district_cl": {"type": "keyword"}, 
            "county_cl": {"type": "keyword"}, 
            "koebstad_cl": {"type": "keyword"}, 
            "island_cl": {"type": "keyword"}, 
            "town_cl": {"type": "keyword"}, 
            "place_cl": {"type": "keyword"}, 
            "county_std": {"type": "keyword"}, 
            "parish_std": {"type": "keyword"}
        }
    }

def mappings_index_lifecourses():
    return {
        "dynamic": False,
        "properties": {
            "id": {"type": "integer"},
            "personal_appearance": mapping_pa()
        }
    }

def mappings_index_links():
    return {
        "dynamic": False,
        "properties": {
            "id": {"type": "integer"},
            "personal_appearance": mapping_pa(),
            "method": {"type": "keyword"},
            "score": {"type": "float"}
        }
    }

def mappings_index_pas():
    return {
        "dynamic": False,
        "properties": {
            "id": {"type": "integer"},
            "personal_appearance": mapping_pa()
        }
    }


if __name__ == "__main__":
    import sys
    import os
    #es = Elasticsearch(hosts=["52.215.59.213:1234", "52.215.59.213:9300"])
    es = Elasticsearch(hosts=["localhost:9200", "localhost:9300"])
    if len(sys.argv) == 2 and sys.argv[1] == "setup":
        print("deleting indices")
        try:
            es.indices.delete("links,lifecourses,pas")
        except:
            pass

        print("setting up indices")
        print(" => creating links index")
        es.indices.create('links')
        print(" => putting links mapping")
        es.indices.put_mapping(index='links', body=mappings_index_links())

        print(" => creating lifecourses index")
        es.indices.create('lifecourses')   
        print(" => putting lifecourse mapping")
        es.indices.put_mapping(index='lifecourses', body=mappings_index_lifecourses())         

        print(" => creating pas index")
        es.indices.create('pas')
        print(" => putting pas mapping")
        es.indices.put_mapping(index='pas', body=mappings_index_pas())
    elif len(sys.argv) == 3 and os.path.exists(sys.argv[2]):
        print(f'indexing {sys.argv[2]}')
        index(sys.argv[2], es)
    else:
        print('argument error')
        sys.exit(1)