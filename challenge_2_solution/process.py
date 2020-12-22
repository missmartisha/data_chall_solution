import json
import logging
import os
import sys
import sqlstring as ss


def main():
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Processing of sql files in folder ../sql_scripts has started.")

    try:

        keywords = dict()
        # convert relative path to abs path
        my_path = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(my_path, "../sql_scripts/")

        # process file by file from the path
        for entry in os.scandir(path):
            if entry.path.endswith(".sql"):
                table_name, fields = ss.process_file(entry,keywords)
                try:
                    keywords.update({table_name: fields})
                except:
                    keywords[table_name] = fields
                    
        # write dictionary to json config
        with open('table_fields.json', 'w') as tf:
            json.dump(keywords, tf)
        
        logging.info("Processing is complete.")

    except Exception as err:
        logging.info("Processing of sql files failed")
        logging.error(err)

if __name__ == "__main__":
    main()
