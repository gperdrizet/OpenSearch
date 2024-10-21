'''Runs Luigi ETL pipeline for semantic search database creation in OpenSearch.'''

# Standard library imports
import json
from pathlib import Path

# PyPI imports
import luigi # type: ignore

# Internal imports
import semantic_search.configuration as config
import semantic_search.classes.luigi_tasks as tasks
import semantic_search.functions.luigi_helper as helper_funcs

if __name__ == '__main__':

    # Parse command line arguments
    args=helper_funcs.parse_arguments()

    # Make output directory for intermediate files
    output_data_path=f"{config.DATA_PATH}/{args.data_source}"
    Path(output_data_path).mkdir(parents=True, exist_ok=True)

    # Require restart of pipeline from intermediate job, if asked
    helper_funcs.force_from(args.data_source, args.force_from)

    luigi.build(
        [
            # Extract and batch text from raw data
            tasks.ExtractText(data_source=args.data_source),
            # Clean and semantically split extracted text
            tasks.ParseText(data_source=args.data_source),
            # Calculates embedding vectors for cleaned and chunked text
            tasks.EmbedText(data_source=args.data_source),
            # Load data into OpenSearch KNN vector database
            tasks.LoadText(data_source=args.data_source)
        ],
        local_scheduler=True
    )
