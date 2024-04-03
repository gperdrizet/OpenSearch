import ijson # type: ignore

def run():
    
    # Open the JSON file
    with open('data/enwiki-20240325-cirrussearch-content.json', 'r') as file:
        # Parse the JSON objects one by one
        parser = ijson.items(file, 'item')

        print(f'Parser is {type(parser)}')
        
        # Iterate over the JSON objects
        i = 0 
        for item in parser:
            # Process each JSON object as needed
            print(item)

            if i > 0:
                break

            i+=1