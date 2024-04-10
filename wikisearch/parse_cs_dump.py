import json
from gzip import GzipFile
from multiprocessing import Manager, Process
from threading import Thread

from wikisearch.classes.cirrussearch_reader import CirrusSearchReader
import wikisearch.functions.IO_functions as io_funcs
import wikisearch.functions.parsing_functions as parse_funcs

################################################################################

def run(
    input_file: str,
    index_name: str,
    output_destination: str
) -> None:
    
    '''Main function to run CirrusSearch dump parse'''

    # Start multiprocessing manager
    manager=Manager()

    # Set-up queues
    output_queue=manager.Queue(maxsize=2000)
    input_queue=manager.Queue(maxsize=2000)

    # Initialize the target index
    _=io_funcs.initialize_index(index_name)

    # Open the input file with gzip
    wiki=GzipFile(input_file)

    # Instantiate a CirrusSearch instance, pass our parser's 
    # input queue put function to be used as a callback 
    reader=CirrusSearchReader(input_queue.put)

    # Start the status monitor printout
    status=Thread(
        target=io_funcs.display_status, 
        args=(input_queue, output_queue, reader)
    )

    status.start() 

    # Start parser jobs
    for _ in range(1):

        parse_process=Process(
            target=parse_funcs.parse_cirrussearch_article, 
            args=(input_queue, output_queue, index_name)
        )

        parse_process.start()

    # Target the correct output function

    # Start writer jobs
    for _ in range(10):

        # Save to file
        if output_destination == 'file':

            write_process=Process(
                target=io_funcs.write_file, 
                args=(output_queue, 'cirrus_search')
            )

        # Insert to OpenSearch
        elif output_destination == 'opensearch':

            write_process=Process(
                target=io_funcs.bulk_index_articles, 
                args=(output_queue, index_name)
            )

    # # Save to file
    # if output_destination == 'file':

    #     write_thread=Thread(
    #         target=io_funcs.write_file, 
    #         args=(output_queue, 'cirrus_search')
    #     )

    # # Insert to OpenSearch
    # elif output_destination == 'opensearch':

    #     write_thread=Thread(
    #         target=io_funcs.bulk_index_articles, 
    #         args=(output_queue, index_name)
    #     )

        # Not sure what to do - warn user
        else:
            print(f'Unrecognized output destination: {output_destination}.')

        # Start the output writer thread
        write_process.start()

    # Send the XML data stream to the reader via xml's sax parser
    for line in wiki:

        reader.read_line(line)
