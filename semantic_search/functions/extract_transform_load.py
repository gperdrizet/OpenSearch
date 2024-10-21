'''Data pipeline functions, meant to be called by Luigi tasks.'''

# Standard imports
import json

# Internal imports
import semantic_search.configuration as config
from functions.extraction.wikipedia import wikipedia_extractor


def extract_text(data_source: str) -> dict:
    '''Wrapper function to call correct task specific text extractor function.'''

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Pick the extractor function to run based on the data source configuration
    extractor_function=globals()[source_config['extractor_function']]

    # Run the extraction
    extraction_summary=extractor_function(source_config)

    return extraction_summary
