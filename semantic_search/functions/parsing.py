'''Functions for data cleaning and chunking. Meant to be
run by multiprocessing pool workers'''

# Standard imports
import multiprocessing as mp

# PyPI imports
from semantic_text_splitter import TextSplitter
from tokenizers import Tokenizer

# Internal imports
import semantic_search.configuration as config


def parse_text(reader_queue: mp.Queue, writer_queue: mp.Queue) -> None:
    '''Takes records from reader queue. Cleans and semantically 
    splits text, sends parsed texts to writer process for batching
    and output to hdf5.'''

    # Fire up the semantic chunk splitter
    tokenizer=Tokenizer.from_pretrained(config.TOKENIZER_NAME)
    splitter=TextSplitter.from_huggingface_tokenizer(tokenizer, config.MAX_TOKENS)

    # Main loop
    while True:
        
        # Get the next record from the reader queue.
        record=reader_queue.get()

        # Break out of the main loop when the reader process tells us we are done.
        if record == 'done':
            break

        # Strings come out of hdf5 as bytes
        text=record.decode('utf-8')

        # Do some string replacements
        text=fix_bad_symbols(text)

        # Clean up newlines
        text=clean_newlines(text)

        # Split the text into chunks
        chunks=splitter.chunks(text)

        # Send each chunk to the writer process
        for chunk in chunks:
            writer_queue.put(chunk)

    # When we break out of the main loop due to a 'done' signal from the 
    # reader process, send the same done signal along to the writer
    # process.
    writer_queue.put('done')

    # Finished
    return


def fix_bad_symbols(source_string: str) -> str:
    '''Fixes some weird punctuation and symbols left over after
    code is stripped by mwparserfromhell'''

    source_string=source_string.replace('–', '-')
    source_string=source_string.replace('(/', '(')
    source_string=source_string.replace('/)', ')')
    source_string=source_string.replace('(, ', '(')
    source_string=source_string.replace('( , ; ', '(')
    source_string=source_string.replace(' ', ' ')
    source_string=source_string.replace('′', '`')
    source_string=source_string.replace('(: ', '(')
    source_string=source_string.replace('(; ', '(')
    source_string=source_string.replace('( ', '(')
    source_string=source_string.replace(' )', ')')
    source_string=source_string.replace('皖', '')
    source_string=source_string.replace('()', '')
    source_string=source_string.replace('(;)', '')
    source_string=source_string.replace(' ; ', '; ')
    source_string=source_string.replace('(,', '(')
    source_string=source_string.replace(',)', ')')
    source_string=source_string.replace(',),', ',')
    source_string=source_string.replace(',“', ', "')
    source_string=source_string.replace('( ;)', '')
    source_string=source_string.replace('(;', '(')
    source_string=source_string.replace(' .', '.')
    source_string=source_string.replace(';;', ';')
    source_string=source_string.replace(';\n', '\n')
    source_string=source_string.replace(' ,', ',')
    source_string=source_string.replace(',,', ',')
    source_string=source_string.replace('−', '-')
    source_string=source_string.replace('۝ ', '')
    source_string=source_string.replace('۝', '')
    source_string=source_string.replace("\'", "'")

    # Need to do this one last, some of the above
    # replace-with-nothings leave double spaces
    source_string=source_string.replace('  ', ' ')

    return source_string


def clean_newlines(source_string: str) -> str:
    '''Fixes up some issues with multiple newlines'''

    source_string=source_string.replace(' \n', '\n')
    source_string=source_string.replace('\n\n\n\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n', '\n')
    source_string=source_string.replace('\n\n\n', '\n\n')

    return source_string
