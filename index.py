import sqlite3
from elasticsearch import Elasticsearch
from neo4j import GraphDatabase, basic_auth
from math import ceil

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

        props = list(map(dict, c.execute(f'SELECT DISTINCT C.* FROM {table_name} C LEFT JOIN Links L ON L.pa_id = C.pa_id AND L.source_id = {source_id} WHERE L.link_id IS NOT NULL')))
        for prop in props:
            prop['source_id'] = source_id
        neo4j.run('UNWIND $props AS prop CREATE (pa:PersonAppearance) SET pa = prop', props=props)

def index_graph_links(sqlite_db, neo4j):
    with sqlite3.connect(sqlite_db) as sqlite:
        sqlite.row_factory = sqlite3.Row
        c = sqlite.cursor()
        for row in c.execute(f"SELECT L.link_id, L.score, LC.life_course_id, group_concat(L.pa_id, ',') as pa_ids, group_concat(L.source_id, ',') as source_ids FROM Links L LEFT JOIN Life_courses LC ON LC.link_id = L.link_id GROUP BY L.link_id"):
            pa_id_a, pa_id_b = row['pa_ids'].split(',')
            source_id_a, source_id_b = row['source_ids'].split(',')
            life_course_id = row['life_course_id']
            score = row['score']
            link_id = row['link_id']
            neo4j.run(f'''
            MATCH (a:PersonAppearance),(b:PersonAppearance)
            WHERE a.pa_id = {pa_id_a} AND b.pa_id = {pa_id_b} AND a.source_id = {source_id_a} AND b.source_id = {source_id_b}
            CREATE (a)-[r:LifeLink{{link_id: {link_id}, score: {score}, life_course_id: {life_course_id} }}]->(b)
            ''')


def index_nested(sqlite_db, es):
    with open('lifecourses_full_sorted.sql', 'r', encoding='utf-8') as sqlfile:
        query = sqlfile.read()

    with sqlite3.connect(sqlite_db) as sqlite:
        sqlite.row_factory = sqlite3.Row
        c = sqlite.cursor()
        last_life_course_id = None
        life_course = []
        for row in c.execute(query):
            if life_course and last_life_course_id != row['life_course_id']:
                index_nested_lifecourse(life_course)
                life_course = []
            life_course.append(row)
            last_life_course_id = row['life_course_id']
            

def index_nested_lifecourse(life_course):
    life_course = {
        "pa": list(map(lambda row: {key: row[key] for key in dict(row) if row[key] != ""}, life_course))
    }
    es.index(index='link-lives', id=life_course['pa'][0]['life_course_id'], body=life_course)

def index_flat(sqlite_db, es):
    with open('lifecourses_full_sorted.sql', 'r', encoding='utf-8') as sqlfile:
        query = sqlfile.read()

    with sqlite3.connect(sqlite_db) as sqlite:
        sqlite.row_factory = sqlite3.Row
        c = sqlite.cursor()
        for row in c.execute(query):
            es.index(index='link-lives', id=f"{row['life_course_id']}-{row['pa_id']}-{row['source_id']}", body=dict(row))



if __name__ == "__main__":
    import sys
    if sys.argv[1] == 'es':
        es = Elasticsearch()
        index_nested('LL.db', es)
    elif sys.argv[1] == 'graph':
        driver = GraphDatabase.driver('bolt://localhost', auth=basic_auth('neo4j', '123456'))
        session = driver.session()
        if sys.argv[2] == "nodes":
            print("indexing nodes")
            index_graph_linked_nodes('LL.db', session, "census_1860", 0)
            index_graph_linked_nodes('LL.db', session, "census_1850", 1)
            index_graph_linked_nodes('LL.db', session, "census_1845", 2)
        elif sys.argv[2] == "links":
            print("indexing links")
            index_graph_links('LL.db', session)