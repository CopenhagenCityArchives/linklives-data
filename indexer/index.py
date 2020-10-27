import sqlite3
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError as ESNotFoundError
from math import ceil
from pathlib import Path


CHUNK_SIZE = 10000
PA_IGNORE_KEYS = ["life_course_id", "link_id", "method_id", "score"]


def index(sqlite_db, es):
    print(" => reading sources")
    sources = list(read_sources(sqlite_db))

    print(" => creating source readers")
    readers = {source['source_id']: {'reader': read_source_chunks(sqlite_db, source), 'pointer': 0, 'data': None} for source in sources}

    print(" => indexing data")
    life_course_count = 0
    pa_count = 0
    link_count = 0
    print_counter = 0
    while True:
        print_counter += 1
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

        for link_id in links:
            index_link(link_id, life_course_id, links[link_id])
            link_count += 1

        index_life_course(life_course_id, life_course)
        life_course_count += 1

        if print_counter == 100:
            print_counter = 0
            print(f" => person appearances: {pa_count}, links: {link_count}, life courses: {life_course_count}", end="\r")
    print(f" => person appearances: {pa_count}, links: {link_count}, life courses: {life_course_count}")


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
        if data is not None and data['life_course_id'] == life_course_id:
            life_course.append(data)
            readers[source_id]['pointer'] += 1

            # index the pa when added to life course, because it wont be read anymore
            index_pa(data)
        else:
            # go to next reader to prevent unneccesary double check
            continue

        # handle pas being part of two links by getting the next too
        data = reader_peek(readers, source_id)
        if data is not None and data['life_course_id'] == life_course_id:
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
            yield list(c.execute(f"SELECT * FROM {source['table_name']} p JOIN Links l ON l.source_id = {source['source_id']} AND l.pa_id = p.pa_id JOIN Life_courses lc ON lc.link_id = l.link_id ORDER BY lc.life_course_id, l.link_id, p.pa_id ASC LIMIT {offset},{CHUNK_SIZE}"))
            offset += CHUNK_SIZE


def index_pa(pa):
    doc = {
        "person_appearance": {
            key: pa[key]
            for key in dict(pa)
            if key not in PA_IGNORE_KEYS and pa[key] != ""
        }
    }
    es.index(index="pas", id=f"{pa['source_id']}-{pa['pa_id']}", body=doc)


def index_link(link_id, life_course_id, pas):
    doc = {
        'link_id': link_id,
        'life_course_ids': [life_course_id],
        'person_appearance': []
    }
    for pa in pas:
        doc['person_appearance'].append({
            key: pa[key]
            for key in dict(pa)
            if key not in PA_IGNORE_KEYS and pa[key] != ""
        })
        
    # if link does not exist, index it, otherwise just add life course id
    body = {
        "upsert": doc,
        "script": {
            "source": "ctx._source.life_course_ids.add(params.life_course_id)",
            "lang": "painless",
            "params": {
                "life_course_id": life_course_id
            }
        }
    }
    es.update(index="links", id=link_id, body=body)


def index_life_course(life_course_id, pas):
    doc = {
        'life_course_id': life_course_id,
        'person_appearance': []
    }
    for pa in pas:
        # skip duplicates
        if any([doc_pa['source_id'] == pa['source_id'] and doc_pa['pa_id'] == pa['pa_id'] for doc_pa in doc['person_appearance']]):
            continue
        doc['person_appearance'].append({
            key: pa[key]
            for key in dict(pa)
            if key not in PA_IGNORE_KEYS and pa[key] != ""
        })
    es.index(index='lifecourses', id=life_course_id, body=doc)


def mapping_pa_properties():
    """Returns the Elasticsearch mappings for person appearance objects."""
    return {
        "pa_id": {"type": "integer"},
        "source_id": {"type": "integer"},

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

def mappings_index_lifecourses():
    """Returns the Elasticsearch mappings for the 'lifecourses' index
    containing the life courses."""
    return {
        "dynamic": False,
        "properties": {
            "life_course_id": {"type": "integer"},
            "person_appearance": {
                "type": "nested",
                "properties": mapping_pa_properties()
            }
        }
    }

def mappings_index_links():
    """Returns the Elasticsearch mappings for the 'links' index containing the
    links between person appearances in different sources."""
    return {
        "dynamic": False,
        "properties": {
            "link_id": {"type": "integer"},
            "method": {"type": "keyword"},
            "score": {"type": "float"},
            "life_course_ids": {"type": "integer"},
            "person_appearance": {
                "type": "nested",
                "properties": mapping_pa_properties()
            }
        }
    }

def mappings_index_pas():
    """Returns the Elasticsearch mappings for the 'pas' index containing person
    appearances.
    """

    return {
        "dynamic": False,
        "properties": {
            "person_appearance": {
                "type": "nested",
                "properties": mapping_pa_properties()
            }
        }
    }


def read_csv(path, delimiter='$'):
    """Read a simple comma-separated file with arbitrary separators.

    Does *NOT* support multiline or quoted values.

    Args:
        path: A string containing the path to the csv-file to open for reading.
        delimiter: The delimiter used to separate values on each line.
    """

    headers = None
    with open(path, 'r', encoding='utf-8') as f:
        headers = next(f).strip().split(delimiter)
        for line in f:
            yield { header: None if value == '' else value for (header, value) in zip(headers, line.strip().split(delimiter)) }


def source_info(source_id):
    """Get the source information from the id.

    Args:
        source_id: The unique identifier of the source.

    Returns:
        A dictionary containing the source metadata information, ie. the 'year'
        and 'type' of the source.
    """
    sources = {
        '0': { 'year': 1787, 'type': 'census' },
        '1': { 'year': 1801, 'type': 'census' },
        '2': { 'year': 1834, 'type': 'census' },
        '3': { 'year': 1840, 'type': 'census' },
        '4': { 'year': 1845, 'type': 'census' },
        '5': { 'year': 1850, 'type': 'census' },
        '6': { 'year': 1860, 'type': 'census' },
        '7': { 'year': 1880, 'type': 'census' },
        '8': { 'year': 1885, 'type': 'census' },
        '9': { 'year': 1901, 'type': 'census' }
    }
    
    return sources[str(source_id)]


def csv_index_life_course(es, life_course):
    """Index a document in the 'life_courses' index.
    
    This only creates the empty life course document without any person
    appearances, which are added to this index by the `csv_index_pa` function.

    Args:
        es: An Elasticsearch client
        life_course: The life course object
    """
    doc = {
        'life_course_id': life_course[''],
        'person_appearance': []
    }

    es.index(index="lifecourses", id=life_course[''], body=doc)


def csv_index_link(es, link):
    """Index a document in the 'links' index.

    This only creates the link document with link metadata, without any person
    appearances, which are added to this index by the `csv_index_pa` function.
    
    Args:
        es: An Elasticsearch client
        link: The link object
    """
    pass


def csv_index_pa(es, pa, links, life_courses):
    """Index a person appearance.
        
    Indexes a document in the 'pas' index, and potentially adds the person
    appearance to the 'life_course' index documents that the person appearance
    belongs to.

    Args:
        es: An Elasticsearch client
        pa: The person appearance object
        links: A list of link ids this person appearance belongs to
        life_courses: A list of life course ids this person appearance belongs to
    """
    pa = {
        'pa_id': pa['id'],
        'gender': pa['gender'],
        'age': pa['age'],
        'age_clean': pa['age_clean'],
        'name': pa['name'],
        'name_std': pa['name_std']
    }

    # index pa into pas
    es.index(index='pas', id=pa['id'], body={'person_appearance': pa})
    
    # index pa into links

    # index pa into life courses
    for life_course_id in life_courses:
        print(life_course_id, pa['name'])
        body = {
            "script": {
                "source": "ctx._source.person_appearance.add(params.pa)",
                "lang": "painless",
                "params": {
                    "pa": pa
                }
            }
        }
        es.update(index="lifecourses", id=life_course_id, body=body)


def csv_index(es, path):
    """Perform the indexing of a directory of link lives data.

    Args:
        es: An Elasticsearch client
        path: Path to the directory containing life course, link and source data.
    """
    csv_dir = Path(path)
    life_courses = {}
    links = {}
    pa_life_courses = {}
    pa_links = {}

    for csv in [f for f in csv_dir.iterdir() if f.suffix == '.csv' and f.stem.startswith('life_courses')]:
        print('loading life course data from',csv)
        for item in read_csv(csv):
            life_course_id = item['']

            # add the life course to the life courses dict
            life_courses[life_course_id] = item

            # extract the columns of the life course csv that are pa_ids
            pa_ids_src = [(key, val) for (key, val) in item.items() if val is not None and key not in ('', 'occurences')]

            # add each pa_id-source_year combination to the pa_life_course dict
            for (source_year, pa_id) in pa_ids_src:
                if (pa_id, source_year) not in pa_life_courses:
                    pa_life_courses[(pa_id, source_year)] = set()
                pa_life_courses[(pa_id, source_year)].add(life_course_id)

    print(f'loaded {len(life_courses)} life courses')
    for csv in [f for f in csv_dir.iterdir() if f.suffix == '.csv' and f.stem.startswith('links')]:
        print('loading link data from',csv)
        for item in read_csv(csv):
            link_id = item['link_id']

            # add the link to the link dict
            links[link_id] = item

            # add the pa_ids to the pa_links dictionary
            # get info for the first pa in the link
            pa_id_1 = item['pa_id1']
            source_id_1 = item['source_id1']
            source_1 = source_info(source_id_1)

            # get info for the secoond pa in the link
            pa_id_2 = item['pa_id2']
            source_id_2 = item['source_id2']
            source_2 = source_info(source_id_2)

            # add each info to the pa_links dictioanry
            for pa_id, source_year in [(pa_id_1, source_1['year']), (pa_id_2, source_2['year'])]:
                if (pa_id, source_year) not in pa_links:
                    pa_links[(pa_id, source_year)] = set()
                pa_links[(pa_id, source_year)].add(link_id)

    print(f'indexing empty life courses')
    for life_course in life_courses.values():
        csv_index_life_course(es, life_course)

    print(f'indexing empty links')
    for link in links.values():
        csv_index_link(es, link)

    print(f'loaded {len(links)} links')
    #for csv in [f for f in csv_dir.iterdir() if f.suffix == '' and f.stem.startswith('census')]:
    for csv in [f for f in csv_dir.iterdir() if f.stem in ('census_1901', 'census_1885')]:
        print('indexing census data from',csv)
        for item in read_csv(csv):
            # a lot of exceptions occured because 'age' was set to the string 'Under 1 Aar', which
            # clashed with the Elasticsearch index mapping which typed 'age' float.
            if item.get('age') == 'Under 1 Aar':
                item['age'] = None
            
            # retrieve the life course ids that the person appearance belongs to
            life_course_ids = []
            if (item['id'], item['source_year']) in pa_life_courses:
                life_course_ids = pa_life_courses[(item['id'], item['source_year'])]
            
            link_ids = []
            if (item['id'], item['source_year']) in pa_links:
                link_ids = pa_links[(item['id'], item['source_year'])]

            csv_index_pa(es, item, link_ids, life_course_ids)


if __name__ == "__main__":
    import sys
    import os
    #es = Elasticsearch(hosts=["52.215.59.213:1234", "52.215.59.213:9300"])
    es = Elasticsearch(hosts=["localhost:80"])
    es.info()
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
    elif len(sys.argv) == 3 and sys.argv[1] == 'index' and os.path.exists(sys.argv[2]):
        print(f'indexing {sys.argv[2]}')
        index(sys.argv[2], es)
    elif len(sys.argv) == 3 and sys.argv[1] == 'csv-index' and os.path.exists(sys.argv[2]):
        print(f'indexing csv files at {sys.argv[2]}')
        csv_index(es, sys.argv[2])
    else:
        print('argument error')

        if not os.path.exists(sys.argv[2]):
            print('the specified file does not exist')
        sys.exit(1)