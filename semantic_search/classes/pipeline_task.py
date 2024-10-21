'''Class to hold objects and methods for data source
independent pipeline tasks (ParseText, EmbedText, LoadText)'''

# Standard imports
import time
import json
import multiprocessing as mp
from multiprocessing import Manager, Process

# Internal imports
import semantic_search.configuration as config
import semantic_search.functions.io as io_funcs

# Worker function imports
from semantic_search.functions.worker import parse_text
from semantic_search.functions.worker import embed_text
from semantic_search.functions.worker import make_requests


class PipelineTask():
    '''Abstraction for pipeline task using queue fed worker pool pattern.'''


    def __init__(self, worker_function_name: str, data_source: str, input_file: str, output_file: str, workers: list):

        # Add the target worker function
        self.worker_function_name=worker_function_name

        # Add the worker list
        self.workers=workers

        # Get the worker count
        self.n_workers=len(workers)

        # Set the data source
        self.data_source=data_source

        # Load the data source configuration
        source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

        with open(source_config_path, encoding='UTF-8') as source_config_file:
            self.source_config=json.load(source_config_file)

        # Add the source configuration key-value pairs as class attributes
        for key, value in self.source_config.items():
            setattr(self, key, value)

        # IO paths
        self.input_file_path=f"{config.DATA_PATH}/{self.target_index_name}/{input_file}"
        self.output_file_path=f"{config.DATA_PATH}/{self.target_index_name}/{output_file}"

        # Start multiprocessing manager
        self.manager=Manager()

        # Set-up the task summary as a shared variable via the multiprocessing 
        # manager so that both the reader and writer processes can add values.
        self.task_summary=self.manager.dict(self.source_config)

        # Set-up reader and writer queues to move records from the reader
        # process to the workers and from the workers to the writer process.
        self.reader_queue=self.manager.Queue(maxsize=10000)
        self.writer_queue=self.manager.Queue(maxsize=10000)


    def run(self):
        '''Calls worker function on pool.'''

        # Start the worker pool
        self.pool=mp.Pool(processes=len(self.workers))

        # Define the worker function
        worker_function=globals()[self.worker_function_name]

        # Start each worker
        for worker in self.workers:
            self.pool.apply_async(
                worker_function, (
                    self.reader_queue,
                    self.writer_queue,
                    worker,
                )
            )

        # Start the reader and writer processes to begin the task,
        # timing how long it takes.
        start_time=time.time()
        self.reader_process.start()
        self.writer_process.start()

        # Wait for the pool workers to finish, then shut the pool down.
        self.pool.close()
        self.pool.join()

        # Stop the timer
        dT=time.time() - start_time

        # Clean up IO processes
        self.reader_process.join()
        self.reader_process.close()

        self.writer_process.join()
        self.writer_process.close()

        # Write some stuff to the summary then recover it to a normal python dictionary
        # from the multiprocessing shared memory DictProxy object
        self.task_summary['Processing rate (records per second)']=self.task_summary['Input records read']/dT
        run_time=config.WIKIPEDIA_RECORD_COUNT / self.task_summary['Processing rate (records per second)']
        self.task_summary['Estimated total run time (seconds)']=run_time
        self.task_summary=dict(self.task_summary)

        # Close the queues and stop the manager
        self.manager.shutdown()

        # Finished
        return self.task_summary

    def initialize_hdf5_reader(self):
        '''Set-up reader process: reader gets records from the input
        file and sends them to the workers.'''

        self.reader_process=Process(
            target=io_funcs.hdf5_reader,
            args=(
                self.input_file_path, 
                self.reader_queue, 
                self.n_workers,
                self.task_summary
            )
        )

    def initialize_hdf5_writer(self):
        '''Set-up writer process: takes results from workers
        collects them into batches and writes to output file.'''

        self.writer_process=Process(
            target=io_funcs.hdf5_writer,
            args=(
                self.output_file_path,
                self.output_batch_size,
                self.writer_queue,
                self.n_workers,
                self.task_summary
            )
        )

    def initialize_opensearch_indexer(self):
        '''Set-up OpenSearch indexing process. Collects formatted indexing
        requests from workers into batches, sends them to OpenSearch via
        the bulk ingest interface.'''

        self.writer_process=Process(
            target=io_funcs.indexer,
            args=(
                self.target_index_name,
                config.BULK_INSERT_BATCH_SIZE,
                self.writer_queue,
                self.task_summary
            )
        )