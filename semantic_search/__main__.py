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

    # Load the datasource configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{args.data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Make output directory for intermediate files
    output_data_path=f"{config.DATA_PATH}/{source_config['output_data_dir']}"
    Path(output_data_path).mkdir(parents=True, exist_ok=True)

    helper.force_from(args.data_source, args.force_from)

    luigi.build(
        [
            # Read raw data, extract, batch and save text
            tasks.ExtractRawData(data_source=args.data_source)
        ],
        local_scheduler=True
    )
