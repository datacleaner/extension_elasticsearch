ElasticSearch for DataCleaner
=======================

This is a DataCleaner (http://datacleaner.org) extension for using the ElasticSearch (http://www.elasticsearch.org/) search engine in indexing and searching reference data.

Currently the extension contains these DataCleaner components:

 * ElasticSearch indexer (*Analyze* menu)
   
   This component allows you to build a (new or existing) search index by feeding in records to it. Each record will become a document in the search index. Each column of the record needs to be mapped to a field in the search index.

 * ElasticSearch document ID lookup (*Transform* menu)
   
   Performs a document lookup for each record, based on ID. This transformation is the equivalent of looking up records in a database by their primary key.

 * ElasticSearch full text search (*Transform* menu)
   
   Performs a search for each record, into a search index. The component allows searching across all fields or by setting a specific field to use for matching. The result of the transformation is a Document ID and a Document (represented as a map), which can further be processed by e.g. the built-in Data structures (*Transform* menu) components of DataCleaner.

Please feel free to fork, and to provide feedback in any form.
