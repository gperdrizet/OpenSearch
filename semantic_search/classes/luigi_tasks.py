'''Luigi data ETL task definitions'''

# Standard imports
import json
import multiprocessing as mp

# PyPI imports
import luigi

# Internal imports
import semantic_search.configuration as config
from semantic_search.classes.pipeline_task import PipelineTask
import semantic_search.functions.extract_transform_load as etl_funcs

class ExtractText(luigi.Task):
    '''Runs source specific data extraction function. Reads raw data,
    extracts text and saves in batches.'''

    # Take the data source string as a parameter
    data_source=luigi.Parameter()

    def output(self):

        # Construct output file name for extraction summary file
        extraction_summary_file=(f"{config.DATA_PATH}/"+
            f"{self.data_source}/{config.EXTRACTION_SUMMARY}")

        # Define the extraction summary file as the target for this task
        return luigi.LocalTarget(extraction_summary_file)

    def run(self):

        # Run the extraction
        extraction_summary=etl_funcs.extract_text(self.data_source)

        # Save the extraction summary to disk
        with self.output().open('w') as output_file:
            json.dump(extraction_summary, output_file)


class ParseText(luigi.Task):
    '''Reads extracted data batches, does some text normalization and chunking.'''

    # Take the data source string as a parameter
    data_source=luigi.Parameter()

    def requires(self):
        return ExtractText(self.data_source)

    def output(self):

        # Construct output file name for extraction summary file
        transform_summary_file=(f"{config.DATA_PATH}/"+
            f"{self.data_source}/{config.PARSE_SUMMARY}")

        # Define the parse summary file as the target for this task
        return luigi.LocalTarget(transform_summary_file)

    def run(self):
        '''Runs text normalization and chunking on pre-batched text from extractor.'''

        # Define the worker function to run
        worker_function='parse_text'

        # Set worker count based on avalible CPUs. Subtract three: one
        # for the reader and writer processes and one for the system.
        workers=list(range(mp.cpu_count() - 3))

        # IO paths
        input_file=config.EXTRACTED_TEXT
        output_file=config.PARSED_TEXT

        # Instantiate an instance of PipelineTask
        parsing_task=PipelineTask(
            worker_function,
            self.data_source,
            input_file,
            output_file,
            workers
        )

        # Add a hdf5 reader to the task
        parsing_task.initialize_hdf5_reader()

        # Add a hdf5 writer to the task
        parsing_task.initialize_hdf5_writer()

        # Run the task
        parse_summary=parsing_task.run()

        # Save the transform summary to disk
        with self.output().open('w') as output_file:
            json.dump(parse_summary, output_file)


class EmbedText(luigi.Task):
    '''Uses HuggingFace transformers to pre-calculate embeddings for indexing
    in the next step.'''

    # Take the data source string as a parameter
    data_source=luigi.Parameter()

    def requires(self):
        return ParseText(self.data_source)

    def output(self):

        # Construct output file name for embedding summary file
        embedding_summary_file=(f"{config.DATA_PATH}/"+
            f"{self.data_source}/{config.EMBEDDING_SUMMARY}")

        # Define the parse summary file as the target for this task
        return luigi.LocalTarget(embedding_summary_file)

    def run(self):
        '''Uses HuggingFace transformers to pre-calculate embeddings for indexing.'''

        # Define the worker function to run
        worker_function='embed_text'

        # Set embedding worker count based on GPUs listed in the configuration file
        workers=config.WORKER_GPUS

        # IO paths
        input_file=config.PARSED_TEXT
        output_file=config.EMBEDDED_TEXT

        # Instantiate an instance of PipelineTask
        embedding_task=PipelineTask(
            worker_function,
            self.data_source,
            input_file,
            output_file,
            workers
        )

        # Add a hdf5 reader to the task
        embedding_task.initialize_hdf5_reader()

        # Add a hdf5 writer to the task
        embedding_task.initialize_hdf5_writer()

        # Run the task, returning the task summary
        embedding_summary=embedding_task.run()

        # Save the embedding summary to disk
        with self.output().open('w') as output_file:
            json.dump(embedding_summary, output_file)

class LoadText(luigi.Task):
    '''Loads prepared data into OpenSearch KNN vector database for semantic search.'''

    # Take the data source string as parameter
    data_source=luigi.Parameter()

    def requires(self):
        return ParseText(self.data_source)

    def output(self):

        # Construct output file name for loading summary file
        load_summary_file=(f"{config.DATA_PATH}/"+
            f"{self.data_source}/{config.LOAD_SUMMARY}")

        # Define the load summary file as the target for this task
        return luigi.LocalTarget(load_summary_file)

    def run(self):
        '''Loads embedded text into OpenSearch KNN vector database for semantic search.'''

        # Define the worker function to run
        worker_function='make_requests'

        # Only use one worker to assemble requests for bulk indexing
        workers=[0]

        # IO paths, output file is None because we are sending records
        # to OpenSearch for indexing rather than writing to disk
        input_file=config.EMBEDDED_TEXT
        output_file=None

        # Instantiate an instance of PipelineTask
        indexing_task=PipelineTask(
            worker_function,
            self.data_source,
            input_file,
            output_file,
            workers
        )

        # Add a hdf5 reader to the task
        indexing_task.initialize_hdf5_reader()

        # Add an opensearch indexer for output
        indexing_task.initialize_opensearch_indexer()

        # Run the task, returning the task summary
        load_summary=indexing_task.run()

        # Save the load summary to disk
        with self.output().open('w') as output_file:
            json.dump(load_summary, output_file)
