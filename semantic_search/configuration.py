'''General module level configuration file for pipeline wide
and data source agnostic defaults and paths.'''

import os

# Get path to this config file so that we can define other paths relative to it
PROJECT_ROOT_PATH=os.path.dirname(os.path.realpath(__file__))

# Other project paths
DATA_PATH=f'{PROJECT_ROOT_PATH}/data'
RAW_DATA_PATH=f'{DATA_PATH}/raw_data'
DATA_SOURCE_CONFIG_PATH=f'{PROJECT_ROOT_PATH}/data_source_configurations'

NLTK_ASSET_DIR=f'{PROJECT_ROOT_PATH}/.venv/lib/nltk_data'
TORCH_CACHE='/mnt/fast_scratch/'

# Sematic chunking parameters
TOKENIZER_NAME='bert-base-uncased'
MAX_TOKENS=512

# Embedding parameters
EMBEDDING_MODEL='sentence-transformers/msmarco-distilbert-base-tas-b'
WORKER_GPUS=['cuda:0'] * 6
EMBEDDING_BATCH_SIZE=8
WORKER_BATCHES_PER_ROUND=100

BULK_INSERT_BATCH_SIZE=128

# Default data source to process, can be overridden with command line argument
DEFAULT_DATA_SOURCE='wikipedia'
WIKIPEDIA_RECORD_COUNT=6889224
WIKIPEDIA_ESTIMATED_CHUNK_COUNT=20648877

# Option to force Luigi pipeline to start from a specific task.
# Set 'None' to use default Luigi behavior where we start from the
# last completed task. Can be overridden with command line argument
DEFAULT_FORCE_START='None'

# Luigi task summary files
EXTRACTION_SUMMARY='1.1-extraction_summary.json'
PARSE_SUMMARY='2.1-parse_summary.json'
EMBEDDING_SUMMARY='3.1-embedding_summary.json'
LOAD_SUMMARY='4.1-load_summary.json'

# Intermediate data files
EXTRACTED_TEXT='1.2-extracted_text.h5'
PARSED_TEXT='2.2-parsed_text.h5'
EMBEDDED_TEXT='3.2-embedded_data.h5'
