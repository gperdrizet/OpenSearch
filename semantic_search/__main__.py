'''Runs Luigi ETL pipeline for semantic search database creation in OpenSearch.'''

# Standard library imports
import json
from pathlib import Path

# PyPI imports
import luigi # type: ignore

# Internal imports
import semantic_search.configuration as config
import semantic_search.classes.luigi_tasks as tasks
import semantic_search.functions.luigi_helper as helper
import semantic_search.functions.argument_parser as arg_parser

if __name__ == '__main__':

    # Parse command line arguments
    args=arg_parser.parse_arguments()

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{args.data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Make output directory for intermediate files
    output_data_path=f"{config.DATA_PATH}/{source_config['target_index_name']}"
    Path(output_data_path).mkdir(parents=True, exist_ok=True)

    # Require restart of pipeline from intermediate job, if asked
    helper.force_from(source_config['target_index_name'], args.force_from)

    luigi.build(
        [
            # Extract and batch text from raw data
            tasks.ExtractData(data_source=args.data_source),
            # Clean and semantically split extracted text
            tasks.ParseData(data_source=args.data_source),
            # Calculates embedding vectors for cleaned and chunked text
            tasks.EmbedData(data_source=args.data_source),
            # Load data into OpenSearch KNN vector database
            tasks.LoadData(data_source=args.data_source)
        ],
        local_scheduler=True
    )
