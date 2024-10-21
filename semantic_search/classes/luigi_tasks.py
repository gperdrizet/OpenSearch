'''Luigi data ETL task definitions'''

# Standard imports
import json
import multiprocessing as mp

# PyPI imports
import luigi

# Internal imports
import semantic_search.configuration
from semantic_search.classes.pipeline_task import PipelineTask

class ExtractText(luigi.Task):
    '''Runs source specific data extraction function. Reads raw data,
    extracts text and saves in batches.'''

    # Take the data source string as a parameter and use it to set the
    # data and configuration paths for this task
    data_source=luigi.Parameter()

    def load_data_source_config(self):
        '''Loads data source specific configuration dictionary.'''

        # Load the data source configuration
        source_config_path=f'{semantic_search.configuration.DATA_PATH}/{self.data_source}/2-data_source_configuration.json'
        with open(source_config_path, encoding='UTF-8') as source_config_file:
            source_config=json.load(source_config_file)

        return source_config

    def output(self):

        # Define a task summary file and use it as the target for this task
        data_path=f'{semantic_search.configuration.DATA_PATH}/{self.data_source}'
        extraction_summary_file=f'{data_path}/{semantic_search.configuration.EXTRACTION_SUMMARY}'
        return luigi.LocalTarget(extraction_summary_file)

    def run(self):
        '''Extract data from source.'''

        # Load the data source configuration dictionary
        source_config=self.load_data_source_config()

        # Define the extraction function to run
        worker_function=source_config['extractor_function']

        # Set worker count based on avalible CPUs. Subtract three: one
        # for the reader and writer processes and one for the system.
        workers=list(range(mp.cpu_count() - 3))

        # IO paths
        input_file=source_config['raw_data_file']
        output_file=semantic_search.configuration.EXTRACTED_TEXT

        # Instantiate an instance of PipelineTask
        extraction_task=PipelineTask(
            worker_function,
            self.data_source,
            input_file,
            output_file,
            workers
        )

        # Add data source reader to the task
        extraction_task.initialize_raw_data_reader()

        # Add a hdf5 writer to the task
        extraction_task.initialize_hdf5_writer()

        # Run the task
        extraction_summary=extraction_task.run()

        # Save the extraction summary to disk
        with self.output().open('w') as output_file:
            json.dump(extraction_summary, output_file)


class ParseText(luigi.Task):
    '''Reads extracted data batches, does some text normalization and chunking.'''

    # Take the data source string as a parameter and use it to set the
    # data path for this task
    data_source=luigi.Parameter()

    def requires(self):
        return ExtractText(self.data_source)

    def output(self):
        
        # Define a task summary file and use it as the target for this task
        data_path=f'{semantic_search.configuration.DATA_PATH}/{self.data_source}'
        parse_summary_file=(f'{data_path}/{semantic_search.configuration.PARSE_SUMMARY}')
        return luigi.LocalTarget(parse_summary_file)

    def run(self):
        '''Runs text normalization and chunking on pre-batched text from extractor.'''

        # Define the worker function to run
        worker_function='parse_text'

        # Set worker count based on avalible CPUs. Subtract three: one
        # for the reader and writer processes and one for the system.
        workers=list(range(mp.cpu_count() - 3))

        # IO paths
        input_file=semantic_search.configuration.EXTRACTED_TEXT
        output_file=semantic_search.configuration.PARSED_TEXT

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

    # Take the data source string as a parameter and use it to set the
    # data path for this task
    data_source=luigi.Parameter()

    def requires(self):
        return ParseText(self.data_source)

    def output(self):
    
        # Define a task summary file and use it as the target for this task
        data_path=f'{semantic_search.configuration.DATA_PATH}/{self.data_source}'
        embedding_summary_file=(f'{data_path}/{semantic_search.configuration.EMBEDDING_SUMMARY}')
        return luigi.LocalTarget(embedding_summary_file)

    def run(self):
        '''Uses HuggingFace transformers to pre-calculate embeddings for indexing.'''

        # Define the worker function to run
        worker_function='embed_text'

        # Set embedding worker count based on GPUs listed in the configuration file
        workers=semantic_search.configuration.WORKER_GPUS

        # IO paths
        input_file=semantic_search.configuration.PARSED_TEXT
        output_file=semantic_search.configuration.EMBEDDED_TEXT

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

    # Take the data source string as a parameter and use it to set the
    # data path for this task
    data_source=luigi.Parameter()

    def requires(self):
        return ParseText(self.data_source)

    def output(self):
    
        # Define a task summary file and use it as the target for this task
        data_path=f'{semantic_search.configuration.DATA_PATH}/{self.data_source}'
        load_summary_file=(f'{data_path}/{semantic_search.configuration.LOAD_SUMMARY}')
        return luigi.LocalTarget(load_summary_file)

    def run(self):
        '''Loads embedded text into OpenSearch KNN vector database for semantic search.'''

        # Define the worker function to run
        worker_function='make_requests'

        # Only use one worker to assemble requests for bulk indexing
        workers=[0]

        # IO paths, output file is None because we are sending records
        # to OpenSearch for indexing rather than writing to disk
        input_file=semantic_search.configuration.EMBEDDED_TEXT
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
