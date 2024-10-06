'''Processes dump in XML or CirrusSearch format, indexes to OpenSearch
or writes documents to file.'''

from __future__ import annotations
#import time
import os
import glob
from typing import Union, Callable
from threading import Thread
from multiprocessing import Manager, Process
import wikisearch.functions.helper_functions as helper_funcs
import wikisearch.functions.output_functions as output_funcs

def run(
    input_stream: Union[GzipFile, BZ2File], # type: ignore
    stream_reader: Callable,
    reader_instance: Union[XMLReader, CirrusSearchReader], # type: ignore
    parser_function: Callable,
    args: dict
) -> None:

    '''Main function to parse and upsert dumps'''

    # Start multiprocessing manager
    manager=Manager()

    # Set-up queues
    output_queue=manager.Queue(maxsize=2000)
    input_queue=manager.Queue(maxsize=2000)

    # Add the input queue's put function to the reader class's callback method
    reader_instance.callback=input_queue.put

    # Set up the output sink

    # If we are indexing to OpenSearch, initialize the target index
    if args.output == 'opensearch':
        helper_funcs.initialize_index(args.index)

    # If we are writing to file, set up output directory
    elif args.output == 'file':

        # Construct output path
        if args.task == 'process_xml_dump':
            article_source='xml'

        elif args.task == 'process_cs_dump':
            article_source='cirrussearch'

        else:
            article_source='unknown'

        output_path=f'wikisearch/data/articles/{article_source}'

        print(f'Output path: {output_path}')

        # Clear the output directory
        files=glob.glob(f'{output_path}/*')

        for f in files:
            os.remove(f)

    # Start the status monitor
    Thread(
        target=helper_funcs.display_status,
        args=(input_queue, output_queue, reader_instance, args.status_monitor)
    ).start()

    # Start parser jobs
    for _ in range(args.parse_workers):

        Process(
            target=parser_function,
            args=(input_queue, output_queue, args.index, args.output_workers)
        ).start()

    # Start writer jobs
    for _ in range(args.output_workers):

        # Send output queue to output selector so write traffic
        # gets sent to the correct place
        write_process=Process(
            target=output_funcs.output_selector,
            args=(args, output_queue)
        )

        # Start the output writer thread
        write_process.start()

    # Send the data stream to the reader
    stream_reader(input_stream, reader_instance)

    # Make sure that the queues are empty before exiting
    while True:
        if input_queue.empty() and output_queue.empty():
            break
