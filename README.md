LinkLives Data
==============

This repository contains a tool to index a SQLite database with link-lives data
(person appearances, links and life courses). The indexing can be done
against elasticsearch or neo4j. In both cases the python script `index.py` is
used.

Infrastructure
--------------

It is assumed, depending on which type of indexing is performed, that

 * an elasticsearch instance is running on `localhost` at ports 9200 to 9300,
   or
 
 * a neo4j instance is running on `localhost` and is accessible on the `bolt`
   protocol at port 7687.

A `docker-compose.yml` is provided with services covering these requirements
(services `ll-es`, and `ll-neo4j` respectively).


Dependencies
------------

The following python packages are dependencies

 * neo4j
 * elasticsearch
 * requests

Running the indexing script
---------------------------

The `index.py` script accepts a few different commands

 * `index.py graph nodes` indexes the nodes (ie. person appearances) of the
   link-lives graph. This must be performed before indexing the links.

 * `index.py graph links` indexes the edges (ie. links) of the link-lives
   graph. The nodes must already be indexed.

 * `index.py es setup` creates the elasticsearch indices and sets up the
   mappings of them. This must be done before indexing the documents.

 * `index.py es` indexes the person appearance, link, and life course
   documents in the elasticsearch database. The setup must have created
   the indices beforehand.

Elasticsearch structure
-----------------------

The elasticsearch instance is given three indices: `pas` for person
appearance documents, `links` for link documents, and `lifecourses` for life
course documents.

Each of these indices is given a mapping with a nested property
`person_apperance`, which in the case of the `pas`-index is simply the
person appearance itself. For the `links` and `lifecourses` indices it
contains a list of the related person appearances. This allows nested
querying across the different indices/document types.

### Simple frontend
A simple HTML/native JS frontend for the elasticsearch indices is found in
`browser/browser.html`. A http server running at localhost can be used to
deliver it (due to CORS), for example `python -m http.server` executed in the
`browser` directory.

Neo4j structure
---------------

The neo4j database is indexed with nodes labelled `PersonAppearance` that
contain the person appearance document, and edges labelled `LifeLink`
that contain the link and life course metadata.