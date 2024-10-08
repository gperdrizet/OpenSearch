'''Data pipeline functions, meant to be called by Luigi tasks.'''

# Standard imports
import json

# Internal imports
import semantic_search.configuration as config

def extract_data(data_source: str) -> dict:
    '''Wrapper function to call correct task specific data
    extractor function'''

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Pick the extractor function to run based on the data source configuration
    extractor_function=globals()[source_config['extractor_function']]

    # print(globals())

    # Run the extraction
    extraction_summary=extractor_function(source_config)

    return extraction_summary

def wikipedia_extractor(source_config: dict) -> dict:
    '''Runs text extraction and batching on CirrusSearch Wikipedia dump.'''

    extraction_summary = {
        'raw_data_file': source_config['raw_data_file']
    }

    return extraction_summary
