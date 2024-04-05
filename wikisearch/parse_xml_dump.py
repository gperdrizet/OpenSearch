import time
from multiprocessing import Manager, Process
from bz2 import BZ2File
from threading import Thread
from xml import sax

from wikisearch.classes.wikireader import WikiReader
import wikisearch.functions.parsing_functions as parse_funcs
import wikisearch.functions.IO_functions as io_funcs

################################################################################

def run(
    input_file: str,
    index_name: str,
    output_destination: str
) -> None:
    
    '''Main function to run XML dump parse'''
    
    # Start multiprocessing manager
    manager=Manager()

    # Set-up queues
    output_queue=manager.Queue(maxsize=2000)
    input_queue=manager.Queue(maxsize=2000)
    
    # Open bzip data stream from XML dump file
    wiki=BZ2File(input_file)

    # Instantiate a WikiReader instance, pass it a lambda function
    # to filter record namespaces and our parser's input queue put 
    # function to be used as a callback for when we find article text
    reader=WikiReader(lambda ns: ns == 0, input_queue.put)

    # Start the status monitor printout
    status=Thread(
        target=io_funcs.display_status, 
        args=(input_queue, output_queue, reader)
    )

    status.start() 

    # Start 15 parser jobs
    for _ in range(15):

        process=Process(
            target=parse_funcs.parse_article, 
            args=(input_queue, output_queue)
        )

        process.start()

    # Target the correct output function

    # Save to file
    if output_destination == 'file':

        write_thread=Thread(
            target=io_funcs.write_file, 
            args=(output_queue)
        )

    # Insert to OpenSearch
    elif output_destination == 'opensearch':

        write_thread=Thread(
            target=io_funcs.bulk_index_articles, 
            args=(output_queue, index_name)
        )

    # Not sure what to do - warn user
    else:
        print(f'Unrecognized output destination: {output_destination}.')

    # Start the output writer thread
    write_thread.start()

    # Send the XML data stream to the reader via xml's sax parser
    sax.parse(wiki, reader)