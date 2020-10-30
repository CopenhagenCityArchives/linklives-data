import sqlite3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import RequestError
from math import ceil
from pathlib import Path
import csv


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
    """
    Returns the Elasticsearch mappings for person appearance objects.
    """
    return {
        'gender': {'type': 'text' }, # Gender as transcribed
        'gender_clean': {'type': 'text' }, # Gender after removing unwanted characters
        'gender_std': {'type': 'keyword' }, # Standardized gender. A result of predicting the gender based on the name (also for records not originally coming with a gender)
        'age': {'type': 'string' }, # Age as transcribed
        'age_clean': {'type': 'float' }, # age after cleaning and converting to floats
        'name': {'type': 'text' }, # Name as transcribed
        'name_clean': {'type': 'text' }, # lowercase Name after removing unwanted characters
        'name_std': {'type': 'text' }, # standardized full name
        'first_names': {'type': 'text' }, # standardized names classified as first names
        'family_names': {'type': 'text' }, # standardized names classified as family names
        'patronyms': {'type': 'text' }, # standardized names classified as patronyms
        'uncat_names': {'type': 'text' }, # unclassified standardized names
        'maiden_family_names': {'type': 'text' }, # standardized names classified as maiden family names
        'maiden_patronyms': {'type': 'text' }, # standardized names classified as maiden patronyms
        'all_possible_family_names': {'type': 'text' }, # all possible  family names (standardized names). Includes constructed names based on husband/father names
        'all_possible_patronyms': {'type': 'text' }, # all possible  patronyms (standardized names). Includes constructed names based on husband/father names
        'marital_status': {'type': 'text' }, # marital status as transcribed
        'marital_status_clean': {'type': 'text' }, # marital status after removing unwanted characters
        'marital_status_std': {'type': 'keyword' }, # standardized marital status
        'household_position': {'type': 'text' }, # household position as transcribed
        'household_position_std': {'type': 'keyword' }, # standiardized household position
        'household_family_no': {'type': 'text' }, # household family number as transcribed. Should uniquely label the households. It is far from doing so.
        'hh_id': {'type': 'integer' }, # household id. Improved household identification. Uses multiple variables get a better separation of households.
        'occupation': {'type': 'text' }, # occupation as transcribed. Note: for some censuses the household positions are put here.
        'place_name': {'type': 'text' }, # place_name as transcribed.
        'land_register_address': {'type': 'text' }, # land_register_address  as transcribed.
        'land_register': {'type': 'text' }, # land_register  as transcribed.
        'address': {'type': 'text' }, # address  as transcribed. Note: this rarely contains a full addresss
        'full_address': {'type': 'text' }, # a concatenation of: place_name, land_register_address, land_register, and address
        'parish': {'type': 'text' }, # parish or street where the source was originally created
        'parish_type': {'type': 'keyword' }, # the type of parish, i.e. parish, street etc.
        'district': {'type': 'text' }, # district where the source was originally created
        'county': {'type': 'text' }, # county where the source was originally created
        'state_region': {'type': 'text' }, # state_region (danmark, grønland, færøerne, etc.) where the source was originally created
        'transcription_code': {'type': 'text' }, # unique batch code of the transcription unit
        'transcription_id': {'type': 'integer' }, # unique record number within the transcription unit
        'birth_place': {'type': 'text' }, # birth place as transcribed (note: only available from 1845 and forth)
        'birth_place_clean': {'type': 'text' }, # birth_place after removing unwanted characters
        'birth_place_parish': {'type': 'text' }, # birth place classified as a parish
        'birth_place_district': {'type': 'text' }, # birth place classified as a district
        'birth_place_county': {'type': 'text' }, # birth place classified as a county
        'birth_place_koebstad': {'type': 'text' }, # birth place classified as a koebstad
        'birth_place_town': {'type': 'text' }, # birth place classified as a town
        'birth_place_place': {'type': 'text' }, # birth place classified as a place
        'birth_place_island': {'type': 'text' }, # birth place classified as a island
        'birth_place_other': {'type': 'text' }, # birth place classified as a other (e.g. a country)
        'birth_place_parish_std': {'type': 'text' }, # standardized birth place parish
        'birth_place_county_std': {'type': 'text' }, # standardized birth place county
        'birth_place_koebstad_std': {'type': 'text' }, # standardized birth place koebstad
        'source_reference': {'type': 'text' }, # A reference to the original source
        'transcriber_comments': {'type': 'text' }, # comments by the transcriber
        'source_year': {'type': 'integer' }, # year of the event
        'event_type': {'type': 'text' }, # type of event (e.g. census, burial, baptism, etc.)
        'role': {'type': 'text' }, # the role of the record in the source (e.g. mother, father, child, deceased, bride, etc.)
    }

def mappings_index_lifecourses():
    """
    Returns the Elasticsearch mappings for the 'lifecourses' index
    containing the life courses.
    """
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
    """
    Returns the Elasticsearch mappings for the 'links' index containing the
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
    """
    Returns the Elasticsearch mappings for the 'pas' index containing person
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


def read_csv(path, delimiter='$', quote='"'):
    """
    Read a simple comma-separated file with arbitrary separators.

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


class PersonAppearance:
    """
    An object representing a person appearance.

    Used for parsing and validating person appearances in a single place.
    """

    def __init__(self, pa_id, source_id):
        """
        Initialize a person appearance with just the id properties defined.
        """
        self.id = f'{source_id}-{pa_id}'
        self.pa_id = pa_id
        self.source_id = source_id

        self.gender = None # original	string	census	Gender as transcribed
        self.gender_clean = None # processed	string	census	Gender after removing unwanted characters
        self.gender_std = None # processed	string (m or k)	census	Standardized gender. A result of predicting the gender based on the name (also for records not originally coming with a gender)
        self.age = None # original	string	census	Age as transcribed
        self.age_clean = None # processed	float	census	age after cleaning and converting to floats
        self.name = None # original	string	census	Name as transcribed
        self.name_clean = None # processed	string	census	lowercase Name after removing unwanted characters
        self.name_std = None # processed	string	census	standardized full name
        self.first_names = None # processed	list of strings (comma "," separated)	census	standardized names classified as first names
        self.family_names = None # processed	list of strings (comma "," separated)	census	standardized names classified as family names
        self.patronyms = None # processed	list of strings (comma "," separated)	census	standardized names classified as patronyms
        self.uncat_names = None # processed	list of strings (comma "," separated)	census	unclassified standardized names
        self.maiden_family_names = None # processed	list of strings (comma "," separated)	census	standardized names classified as maiden family names
        self.maiden_patronyms = None # processed	list of strings (comma "," separated)	census	standardized names classified as maiden patronyms
        self.all_possible_family_names = None # processed	list of strings (comma "," separated)	census	all possible  family names (standardized names). Includes constructed names based on husband/father names
        self.all_possible_patronyms = None # processed	list of strings (comma "," separated)	census	all possible  patronyms (standardized names). Includes constructed names based on husband/father names
        self.marital_status = None # original	string	census	marital status as transcribed
        self.marital_status_clean = None # processed	string	census	marital status after removing unwanted characters
        self.marital_status_std = None # processed	string	census	standardized marital status
        self.household_position = None # original	string	census	household position as transcribed
        self.household_position_std = None # processed	string	census	standiardized household position
        self.household_family_no = None # original	string	census	household family number as transcribed. Should uniquely label the households. It is far from doing so.
        self.hh_id = None # processed	integer	census	household id. Improved household identification. Uses multiple variables get a better separation of households.
        self.occupation = None # original	string	census	occupation as transcribed. Note: for some censuses the household positions are put here.
        self.place_name = None # original	string	census	place_name as transcribed.
        self.land_register_address = None # original	string	census	land_register_address  as transcribed.
        self.land_register = None # original	string	census	land_register  as transcribed.
        self.address = None # original	string	census	address  as transcribed. Note: this rarely contains a full addresss
        self.full_address = None # processed	string	census	a concatenation of: place_name, land_register_address, land_register, and address
        self.parish = None # original	string	census	parish or street where the source was originally created
        self.parish_type = None # original	string	census	the type of parish, i.e. parish, street etc.
        self.district = None # original	string	census	district where the source was originally created
        self.county = None # original	string	census	county where the source was originally created
        self.state_region = None # original	string	census	state_region (danmark, grønland, færøerne, etc.) where the source was originally created
        self.transcription_code = None # original	string	census	unique batch code of the transcription unit
        self.transcription_id = None # original	integer	census	unique record number within the transcription unit
        self.birth_place = None # original	string	census	birth place as transcribed (note: only available from 1845 and forth)
        self.birth_place_clean = None # processed	string	census	birth_place after removing unwanted characters
        self.birth_place_parish = None # processed	string	census	birth place classified as a parish
        self.birth_place_district = None # processed	string	census	birth place classified as a district
        self.birth_place_county = None # processed	string	census	birth place classified as a county
        self.birth_place_koebstad = None # processed	string	census	birth place classified as a koebstad
        self.birth_place_town = None # processed	string	census	birth place classified as a town
        self.birth_place_place = None # processed	string	census	birth place classified as a place
        self.birth_place_island = None # processed	string	census	birth place classified as a island
        self.birth_place_other = None # processed	string	census	birth place classified as a other (e.g. a country)
        self.birth_place_parish_std = None # processed	string	census	standardized birth place parish
        self.birth_place_county_std = None # processed	string	census	standardized birth place county
        self.birth_place_koebstad_std = None # processed	string	census	standardized birth place koebstad
        self.source_reference = None # original	string	census	A reference to the original source
        self.transcriber_comments = None # original	string	census	comments by the transcriber
        self.source_year = None # processed	integer	census	year of the event
        self.event_type = None # processed	string	census	type of event (e.g. census, burial, baptism, etc.)
        self.role = None # processed	string	census	the role of the record in the source (e.g. mother, father, child, deceased, bride, etc.)


    def es_document(self):
        """
        Get a dictionary that is an Elasticsearch document.

        Returns:
           A dictionary containing the data of this person appearance in the
           format of an Elasticsearch document. 
        """
        return {
            'id': self.id,
            'pa_id': self.pa_id,
            'source_id': self.source_id,
            'transcription_id': self.transcription_id,
            'gender': self.gender,
            'gender_clean': self.gender_clean,
            'gender_std': self.gender_std,
            'age': self.age,
            'age_clean': self.age_clean,
            'name': self.name,
            'name_clean': self.name_clean,
            'name_std': self.name_std,
            'first_names': self.first_names,
            'patronyms': self.patronyms,
            'family_names': self.family_names,
            'uncat_names': self.uncat_names,
            'maiden_family_names': self.maiden_family_names,
            'maiden_patronyms': self.maiden_patronyms,
            'all_possible_patronyms': self.all_possible_patronyms,
            'all_possible_family_names': self.all_possible_family_names,
            'marital_status': self.marital_status,
            'marital_status_clean': self.marital_status_clean,
            'marital_status_std': self.marital_status_std,
            'household_position': self.household_position,
            'household_position_std': self.household_position_std,
            'household_family_no': self.household_family_no,
            'hh_id': self.hh_id,
            'occupation': self.occupation,
            'place_name': self.place_name,
            'land_register_address': self.land_register_address,
            'parish': self.parish,
            'parish_type': self.parish_type,
            'state_region': self.state_region,
            'county': self.county,
            'district': self.district,
            'transcription_code': self.transcription_code,
            'source_reference': self.source_reference,
            'transcriber_comments': self.transcriber_comments,
            'address': self.address,
            'land_register': self.land_register,
            'source_year': self.source_year,
            'event_type': self.event_type,
            'role': self.role,
            'full_address': self.full_address,
            'birth_place': self.birth_place,
            'birth_place_clean': self.birth_place_clean,
            'birth_place_other': self.birth_place_other,
            'birth_place_parish': self.birth_place_parish,
            'birth_place_district': self.birth_place_district,
            'birth_place_county': self.birth_place_county,
            'birth_place_koebstad': self.birth_place_koebstad,
            'birth_place_island': self.birth_place_island,
            'birth_place_town': self.birth_place_town,
            'birth_place_place': self.birth_place_place,
            'birth_place_county_std': self.birth_place_county_std,
            'birth_place_parish_std': self.birth_place_parish_std,
            'birth_place_koebstad_std': self.birth_place_koebstad_std
        }

    @staticmethod
    def from_dict(data, raise_invalid=False):
        """
        Instantiate a PersonAppearance object from a given dictionary.

        Args:
            pa_id: The person appearance id, unique within a source
            source_id: The source id of the source in which the person
                       appearance occurs
            data: A dictionary containing data about the person appearance.
            raise_invalid: If true, exceptions are raised when invalid
                           properties exist in ``data``. If false these
                           properties are simply skipped.
        
        Returns:
            A PersonAppearance instance.
        """
        pa = PersonAppearance(data['id'], data['source_year'])

        del data['id']

        for prop in data:
            try:
                getattr(pa, prop) # triggers an exception when property does not exist
                setattr(pa, prop, data[prop])
            except AttributeError:
                if raise_invalid:
                    raise
        
        return pa 


class Link:
    """
    A link between two person appearances.
    """

    def __init__(self, link_id, pa_id_1, source_id_1, pa_id_2, source_id_2):
        self.link_id = link_id
        self.pa_id_1 = pa_id_1
        self.pa_id_2 = pa_id_2
        self.source_id_1 = source_id_1
        self.source_id_2 = source_id_2
    
    @staticmethod
    def from_dict(data):
        return Link(data['link_id'], data['pa_id1'], data['pa_id2'], data['source_id1'], data['source_id2'])


class LifeCourse:
    """
    A life course.
    """
    
    def __init__(self, life_course_id):
        """
        Initialize an empty life course.

        Args:
            life_course_id: The unique identifier of the life course
        """
        self.life_course_id = life_course_id
        self.person_appearances = []
        self.occurences = 0
    
    @staticmethod
    def from_dict(data, config):
        lc = LifeCourse(data[''])
        for key in data:
            if key in ('', 'occurences'):
                continue

        return lc

    
def source_info(source_id):
    """
    Get the source information from the id.

    Args:
        source_id: The unique identifier of the source.

    Returns:
        A dictionary containing the source metadata information, ie. the 'year'
        and 'type' of the source.
    """
    sources = {
        '0': { 'year': '1787', 'type': 'census' },
        '1': { 'year': '1801', 'type': 'census' },
        '2': { 'year': '1834', 'type': 'census' },
        '3': { 'year': '1840', 'type': 'census' },
        '4': { 'year': '1845', 'type': 'census' },
        '5': { 'year': '1850', 'type': 'census' },
        '6': { 'year': '1860', 'type': 'census' },
        '7': { 'year': '1880', 'type': 'census' },
        '8': { 'year': '1885', 'type': 'census' },
        '9': { 'year': '1901', 'type': 'census' }
    }
    
    return sources[str(source_id)]


def csv_index_life_courses(es, life_courses, bulk_helper=bulk):
    """
    Bulk indexes documents in the 'life_courses' index.
    
    This only creates the empty life course documents without any person
    appearances, which are added to this index by the `csv_index_pa` function.

    Args:
        es: An Elasticsearch client
        life_courses: An iterable of life course objects
        bulk_helper: Helper function for Elasticsearch _bulk endpoint
    """
    actions = [{'_op_type': 'index', '_index': 'lifecourses', '_id': lc[''], 'life_course_id': lc[''], 'person_appearance': [] } for lc in life_courses]
    bulk_helper(es, actions)


def csv_index_links(es, links, bulk_helper=bulk):
    """
    Bulk indexes documents in the 'links' index.

    This only creates the link documents with link metadata, without any person
    appearances, which are added to this index by the `csv_index_pas` function.
    
    Args:
        es: An Elasticsearch client
        link: The link object
        bulk_helper: Helper function for Elasticsearch _bulk endpoint
    """
    actions = [{'_op_type': 'index', '_index': 'links', '_id': li['link_id'], 'link_id': li['link_id'], 'link': li, 'person_appearance': [] } for li in links]
    bulk_helper(es, actions)


def csv_pa_bulk_actions(pa, life_courses, links):
    """
    Generates the bulk actions for indexing a given person appearance, and
    adding this person appearance to the relevant links and life courses.

    Args:
        pa: A PersonAppearance object
        life_courses: A list of life course ids
        links: A list of link ids
    
    Returns:
        A generator of Elasticsearch bulk actions
    """
    yield {
        '_op_type': 'index',
        '_index': 'pas',
        '_id': pa.id,
        "person_appearance": pa.es_document()
    }

    for link in links:
        yield {
            '_op_type': 'update',
            '_index': 'links',
            '_id': link,
            'script': {
                "source": "ctx._source.person_appearance.add(params.pa)",
                "params": {
                    "pa": pa.es_document()
                }
            }
        }
    
    for life_course in life_courses:
        yield {
            '_op_type': 'update',
            '_index': 'lifecourses',
            '_id': life_course,
            'script': {
                "source": "ctx._source.person_appearance.add(params.pa)",
                "lang": "painless",
                "params": {
                    "pa": pa.es_document()
                }
            }
        }


def csv_pas_bulk_actions(pas):
    """
    Generates bulk actions for the given iterator of PersonAppearance, life
    course ids, and link ids tuples.

    Args:
        pas: A list of tuples containing PersonAppearance objects, lists of life
            course ids and lists of link ids.
        
    Returns:
        A generator of Elasticsearch bulk actions.
    """
    for (pa, life_courses, links) in pas:
        for action in csv_pa_bulk_actions(pa, life_courses, links):
            yield action


def csv_read_pas(csv_files, pa_life_courses, pa_links):
    """
    Reads CSV files containing person appearance data, and generates tuples of
    PersonAppearance objects, lists of life course ids, and lists of link ids.

    Args:
        csv_files: An iterator of pathlib.Path-like objects that can be opened.
        pa_life_courses: A dictionary mapping pa_id to [life_course_id]
        pa_links: A dictionary mapping pa_id to [link_id]

    Returns:
        A generator, generating tuples of PersonAppearance objects, lists of
        life course ids, and lists of link ids
    """
    for csv_path in csv_files:
        print(f' => -> Indexing census data from {csv_path}')
        line = 2
        with csv_path.open('r', encoding='utf-8') as csvfile:
            for item in csv.DictReader(csvfile, delimiter='$', quotechar='"'):
                try:
                    pa = PersonAppearance.from_dict(item)
                except Exception as e:
                    print(f"{repr(e)} line={line} file={csv_path}")
                line += 1
                
                # retrieve the life course ids that the person appearance belongs to
                life_course_ids = []
                if (pa.pa_id, pa.source_id) in pa_life_courses:
                    life_course_ids = pa_life_courses[(pa.pa_id, pa.source_id)]
                
                link_ids = []
                if (pa.pa_id, pa.source_id) in pa_links:
                    link_ids = pa_links[(pa.pa_id, pa.source_id)]

                yield (pa, life_course_ids, link_ids)


def csv_index(es, path):
    """
    Perform the indexing of a directory of link lives data.

    Args:
        es: An Elasticsearch client
        path: Path to the directory containing life course, link and source data.
    """
    csv_dir = Path(path)
    life_courses = {}
    links = {}
    pa_life_courses = {}
    pa_links = {}
    for csv_path in [f for f in csv_dir.iterdir() if f.suffix == '.csv' and f.stem.startswith('life_courses')]:
        print(f' => Loading life course data from {csv_path}')
        with csv_path.open('r', encoding='utf-8') as csvfile:
            for item in csv.DictReader(csvfile, delimiter='$', quotechar='"'):
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
    print(f' => -> Loaded {len(life_courses)} life courses')

    for csv_path in [f for f in csv_dir.iterdir() if f.suffix == '.csv' and f.stem.startswith('links')]:
        print(f' => Loading link data from {csv_path}')
        with csv_path.open() as csvfile:
            for item in csv.DictReader(csvfile, delimiter='$', quotechar='"'):
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
    print(f' => -> Loaded {len(links)} links')

    print(f' => Indexing empty life courses')
    csv_index_life_courses(es, life_courses.values())

    print(f' => Indexing empty links')
    csv_index_links(es, links.values())

    print(f' => Indexing source data')
    pas = csv_read_pas([f for f in csv_dir.iterdir() if f.stem.startswith('census')], pa_life_courses, pa_links)
    bulk(es, csv_pas_bulk_actions(pas))


if __name__ == "__main__":
    import sys
    import os
    import argparse

    parser = argparse.ArgumentParser(description='Index link lives data')

    subparsers = parser.add_subparsers(help='The command to run.', dest='cmd')

    setup_parser = subparsers.add_parser('setup')

    index_parser = subparsers.add_parser('index')
    index_parser.add_argument('--csv-dir', type=lambda p: Path(p).resolve(), required=True)

    index_sqlite_parser = subparsers.add_parser('index-sqlite')
    index_sqlite_parser.add_argument('--sqlite-db', type=lambda p: Path(p).resolve(), required=True)

    args = parser.parse_args()
    
    #es = Elasticsearch(hosts=["52.215.59.213:1234", "52.215.59.213:9300"])
    es = Elasticsearch(hosts=["https://data.link-lives.dk"])
    es.info()
    if args.cmd == 'setup':
        print("Deleting indices")
        try:
            es.indices.delete("links,lifecourses,pas")
        except:
            pass

        print("Setting up indices")
        print(" => Creating links index")
        es.indices.create('links')
        print(" => Putting links mapping")
        es.indices.put_mapping(index='links', body=mappings_index_links())

        print(" => Creating lifecourses index")
        es.indices.create('lifecourses')   
        print(" => Putting lifecourse mapping")
        es.indices.put_mapping(index='lifecourses', body=mappings_index_lifecourses())         

        print(" => Creating pas index")
        es.indices.create('pas')
        print(" => Putting pas mapping")
        es.indices.put_mapping(index='pas', body=mappings_index_pas())
    elif args.cmd == 'index-sqlite':
        if not args.sqlite_db.is_file():
            print(f"Error: Could not find sqlite db {args.sqlite_db}")
            sys.exit(1)
        print(f'Indexing sqlite db {args.sqlite_db}')
        index(str(args.sqlite_db), es)
    elif args.cmd == 'index':
        if not args.csv_dir.is_dir():
            print(f'Error: Path does not exist or is not a directory: {args.csv_dir}')
            sys.exit(1)
        print(f'Indexing csv files at {args.csv_dir}')
        try:
            csv_index(es, str(args.csv_dir))
        except RequestError as e:
            print(f'Error: A request exception occured')
            print(f' => Status code: {e.status_code}, error message: {e.error}')
            print(repr(e.info))
    else:
        print('Error: Invalid command')
        sys.exit(1)