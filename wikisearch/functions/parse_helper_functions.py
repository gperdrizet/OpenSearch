import time
import mwparserfromhell # type: ignore

def parse_article(input_queue, output_queue, shutdown):
    while not (shutdown and input_queue.empty()):
    
        # Get the page title and the content source from the article queue
        page_title, source=input_queue.get()

        # Convert source string to wikicode
        wikicode=mwparserfromhell.parse(source)

        # Strip garbage out of wikicode source
        source_string=wikicode.strip_code(
            normalize=True, 
            collapse=True, 
            keep_template_params=False
        )

        # Before we do anything else, check to see if this is a redirect
        # page. If so, we can just skip it.
        if 'REDIRECT' not in source_string.split('\n')[0]:

            # Remove extra sections from the end of the document
            source_string=remove_extra_sections(source_string)

            # Do some string replacements
            source_string=fix_bad_symbols(source_string)

            # Clean up newlines
            source_string=clean_newlines(source_string)

            # Get rid of image thumbnail lines and leading spaces
            source_string=remove_thumbnails(source_string)

            # Format page title for use as a filename
            filename=page_title.replace(' ', '_')
            filename=filename.replace('/', '-')

            # Put the result into the output queue
            output_queue.put((filename, source_string))


def display_status(input_queue, output_queue, reader):
    '''Prints queue sizes every second'''

    print('\n\n\n')

    while True:
        print('\033[F\033[F\033[F', end='')
        print(f'Input queue size: {input_queue.qsize()}')
        print(f'Output queue size: {output_queue.qsize()}')
        print(f'Reader count: {reader.status_count}')
        time.sleep(1)

def write_out(output_queue, shutdown):

    while not (shutdown and output_queue.empty()):

        # Get article from queue
        output=output_queue.get()

        # Extract filename and text
        filename=output[0]
        text=output[1]

        # Save article to a file
        with open(f"wikisearch/data/articles/{filename}", 'w') as text_file:
            text_file.write(text)


def fix_bad_symbols(source_string):
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

    # Need to do this one last, some of the above
    # replace-with-nothings leave double spaces
    source_string=source_string.replace('  ', ' ')

    return source_string

def clean_newlines(source_string):
    '''Fixes up some issues with multiple newlines'''
            
    source_string=source_string.replace(' \n', '\n')
    source_string=source_string.replace('\n\n\n\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n', '\n')

    return source_string

def remove_thumbnails(source_string):
    '''Removes thumbnail descriptor lines and cleans up any
    lines with leading spaces'''

    # Empty list for cleaned lines
    cleaned_source_array=[]

    # Split the source string on newlines
    source_array=source_string.split('\n')

    # Loop on the line array
    for line in source_array:

        # Only take lines that don't contain leftover HTML stuff
        if 'thumb|' in line:
            pass

        if 'scope="' in line:
            pass

        if 'rowspan="' in line:
            pass

        if 'style="' in line:
            pass
            
            # Check for lines that are not blank but start with space
            # or other garbage
            if len(line) > 1:

                if line[0] == ' ':
                    line=line[1:]

                if line[:2] == '| ':
                    line=line[2:]

                if line[:2] == '! ':
                    line=line[2:]

                if line[:2] == '! ':
                    line=line[2:]

                if line[:2] == '|-':
                    line=line[2:]

                if line[:2] == '|}':
                    line=line[2:]

            # Add the cleaned line to the result
            cleaned_source_array.append(line)

    # Join the cleaned lines back to a string
    source_string='\n'.join(cleaned_source_array)

    return source_string

def remove_extra_sections(source_string):
    '''Remove extra sections from the end of the document by splitting 
    on common headings only keeping the stuff before the split point'''
    
    source_string=source_string.split('See also')[0]
    source_string=source_string.split('References')[0]
    source_string=source_string.split('External links')[0]
    source_string=source_string.split('Notes')[0]

    return source_string