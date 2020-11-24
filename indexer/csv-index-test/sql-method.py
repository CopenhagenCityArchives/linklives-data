
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

elif args.cmd == 'index-sqlite':
        es = Elasticsearch(hosts=[args.es_host])
        if not args.sqlite_db.is_file():
            print(f"Error: Could not find sqlite db {args.sqlite_db}")
            sys.exit(1)
        print(f'Indexing sqlite db {args.sqlite_db}')
        index(str(args.sqlite_db), es)
        
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

