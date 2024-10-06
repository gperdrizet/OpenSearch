'''Default configuration. Parameters here can be overridden via
command line arguments. This file exists to collect defaults
in one, easy-to-read place.'''

# Index settings and parameters
INDEX_TYPE='neural' # or keyword

# Ingest pipeline

# Model is DistilBERT set up via the OpenSearch dashboard dev tools
# for more info. see 04-OpenSearch_neural_search.md under /notes
MODEL_ID='cTrrXpIBaoSTziCbUV0w'
MODEL_GROUP_ID='bzrqXpIBaoSTziCblV3v'

NLP_INGEST_PIPELINE_ID='nlp_ingest_pipeline'
NLP_INGEST_PIPELINE_DESCRIPTION='An NLP ingest pipeline'

# Default number of workers to start for parsing documents, can be overridden
# via command line argument
XML_PARSE_WORKERS=1
CS_PARSE_WORKERS=1

# Default number of workers to start for outputting parsed
# documents to file or OpenSearch index can be overridden
# via command line argument
XML_OUTPUT_WORKERS=4
CS_OUTPUT_WORKERS=4

# Default number of documents to index via bulk call to OpenSearch
# can be overridden via command line argument
BULK_BATCH_SIZE=5

# Default dump data files can be overridden via command line argument
XML_INPUT_FILE='wikisearch/data/enwiki-20240320-pages-articles-multistream.xml.bz2'
CS_INPUT_FILE='wikisearch/data/enwiki-20240401-cirrussearch-content.json.gz'

# OpenSearch index names can be overridden via command line argument
XML_INDEX='enwiki_xml'
CS_INDEX='enwiki_cs'

# Index to use for search test
TEST_SEARCH_INDEX='enwiki_xml'