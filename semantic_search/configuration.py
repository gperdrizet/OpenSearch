'''General module level configuration file for pipeline wide
and data source agnostic defaults and paths.'''

import os

# Get path to this config file so that we can define other paths relative to it
PROJECT_ROOT_PATH=os.path.dirname(os.path.realpath(__file__))

# Other project paths
DATA_PATH=f'{PROJECT_ROOT_PATH}/data'
RAW_DATA_PATH=f'{DATA_PATH}/raw_data'
DATA_SOURCE_CONFIG_PATH=f'{PROJECT_ROOT_PATH}/data_source_configurations'

# Default data source to process, can be overridden with command line argument
DEFAULT_DATA_SOURCE='wikipedia'

# Option to force Luigi pipeline to start from a specific task.
# Set 'None' to use default Luigi behavior where we start from the
# last completed task
FORCE_START='None'

# Luigi task summary files
EXTRACTION_SUMMARY='extraction_summary.log'
