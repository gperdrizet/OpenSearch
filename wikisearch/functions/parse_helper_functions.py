import time
import mwparserfromhell # type: ignore

def parse_article(input_queue, output_queue, shutdown):
    while not (shutdown and input_queue.empty()):
    
        # Get the page title and the content source from the article queue
        page_title, source = input_queue.get()

        # Convert source string to wikicode
        wikicode = mwparserfromhell.parse(source)

        # Strip garbage out of wikicode source
        source_string = wikicode.strip_code(
            normalize=True, 
            collapse=True, 
            keep_template_params=False
        )

        # Before we do anything else, check to see if this is a redirect
        # page. If so, we can just skip it.
        if 'REDIRECT' not in source_string.split('\n')[0]:

            # Fix dashes
            source_string = source_string.replace('–', '-')

            # Remove extra sections from the end of the document by splitting on common headings
            # only keeping the stuff before the split point
            source_string = source_string.split('See also')[0]
            source_string = source_string.split('References')[0]
            source_string = source_string.split('External links')[0]
            source_string = source_string.split('Notes')[0]

            # Do some string replacements
            source_string = source_string.replace('(/', '(')
            source_string = source_string.replace('/)', ')')
            source_string = source_string.replace('(, ', '(')
            source_string = source_string.replace('( , ; ', '(')
            source_string = source_string.replace(' ', ' ')
            source_string = source_string.replace('′', '`')
            source_string = source_string.replace('(: ', '(')
            source_string = source_string.replace('(; ', '(')
            source_string = source_string.replace('( ', '(')
            source_string = source_string.replace(' )', ')')
            source_string = source_string.replace('皖', '')
            source_string = source_string.replace('()', '')
            source_string = source_string.replace('(;)', '')
            source_string = source_string.replace('  ', ' ')
            source_string = source_string.replace(' ; ', '; ')
            source_string = source_string.replace('(,', '(')
            source_string = source_string.replace(',)', ')')
            source_string = source_string.replace(',),', ',')
            source_string = source_string.replace(' \n', '\n')
            source_string = source_string.replace('\n\n\n\n\n\n', '\n\n')
            source_string = source_string.replace('\n\n\n\n\n', '\n\n')
            source_string = source_string.replace('\n\n\n\n', '\n\n')
            source_string = source_string.replace('\n\n\n', '\n\n')


            # Get rid of image thumbnail lines and a few other things
            cleaned_source_array = []

            source_array = source_string.split('\n')

            for line in source_array:
                if 'thumb|' not in line:
                    
                    if len(line) > 1:
                        if line[0] == ' ':
                            line = line[1:]

                    cleaned_source_array.append(line)

            source_string = '\n'.join(cleaned_source_array)


            # Format page title for use as a filename
            filename = page_title.replace(' ', '_')
            filename = filename.replace('/', '-')

            # Put the result into the file queue for saving
            output_queue.put((filename, source_string))


def display_status(input_queue, output_queue, reader):
    '''Prints queue sizes every second'''

    while True:
        print(f'Article queue size: {input_queue.qsize()}, file queue size: {output_queue.qsize()}, reader count: {reader.status_count}')
        time.sleep(1)

def write_out(output_queue, shutdown):

    while not (shutdown and output_queue.empty()):

        # Get article from queue
        output = output_queue.get()

        # Extract filename and text
        filename = output[0]
        text = output[1]

        # Save article to a file
        with open(f"wikisearch/data/articles/{filename}", 'w') as text_file:
            text_file.write(text)