'''Runs Luigi ETL pipeline for semantic search database creation in OpenSearch.'''

# Standard library imports
from pathlib import Path

# PyPI imports
import luigi

# Internal imports
import semantic_search.configuration
import semantic_search.classes.luigi_tasks
import semantic_search.functions.helper

if __name__ == '__main__':

    # Parse command line arguments
    args=semantic_search.functions.helper.parse_arguments()

    # Make output directory for intermediate files
    data_path=semantic_search.configuration.DATA_PATH
    output_data_path=f"{data_path}/{args.data_source}"
    Path(output_data_path).mkdir(parents=True, exist_ok=True)

    # Require restart of pipeline from intermediate job, if asked
    semantic_search.functions.helper.force_from(args.data_source, args.force_from)

    luigi.build(
        [
            # Extract and batch text from raw data
            semantic_search.classes.luigi_tasks.ExtractText(data_source=args.data_source),
            # Clean and semantically split extracted text
            semantic_search.classes.luigi_tasks.ParseText(data_source=args.data_source),
            # Calculates embedding vectors for cleaned and chunked text
            semantic_search.classes.luigi_tasks.EmbedText(data_source=args.data_source),
            # Load data into OpenSearch KNN vector database
            semantic_search.classes.luigi_tasks.LoadText(data_source=args.data_source)
        ],
        local_scheduler=True
    )
