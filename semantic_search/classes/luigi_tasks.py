'''Luigi data ETL task definitions'''

# Standard imports
import json

# PyPI imports
import luigi

# Internal imports
import semantic_search.configuration as config
import semantic_search.functions.extract_transform_load as etl_funcs

class ExtractRawData(luigi.Task):
    '''Runs source specific data extraction function. Reads raw data,
    extracts text and saves in batches.'''

    # Take the data source string as a parameter
    data_source=luigi.Parameter()

    def load_data_source_config(self):
        '''Loads data source specific configuration dictionary.'''

        # Load the data source configuration
        source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{self.data_source}.json'

        with open(source_config_path, encoding='UTF-8') as source_config_file:
            source_config=json.load(source_config_file)

        return source_config

    def output(self):

        # Construct output file name for extraction summary file
        source_config=self.load_data_source_config()

        extraction_summary_file=(f"{config.DATA_PATH}/"+
            f"{source_config['output_data_dir']}/{config.EXTRACTION_SUMMARY}")

        # Define the extraction summary file as the target for this task
        return luigi.LocalTarget(extraction_summary_file)

    def run(self):

        # Run the extraction
        extraction_summary=etl_funcs.extract_data(self.data_source)

        # Save the extraction summary to disk
        with self.output().open('w') as output_file:
            json.dump(extraction_summary, output_file)
