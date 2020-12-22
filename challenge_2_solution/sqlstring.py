import logging
import re

commentline = re.compile("[-]+")
dbtline = re.compile("[{]+")
quoted = re.compile("'(.*?)'")

def process_file(filename, kw_dict):
    """
    Reads contents of sql file into a string from which it extracts and returns updated list of field keywords along with the table name. 
    Only new fields are added to the list. 
    """
    logging.info(f"Processing {filename}..")
    with open(filename, 'r') as f:
        sql_str = f.read()
    
    before_from, _, after_from = sql_str.partition('\nfrom')
    table_name = process_source(after_from)
    
    try:
        fields_list = kw_dict[table_name]
    except:
        fields_list = []
    
    for i, line in enumerate(filter(None,before_from.splitlines())):
    #     consider only lines that are not comments
        if not commentline.search(line):
    #       differentiate between select line and other 
            if i==0:
                kw = process_line(line.strip('select'))
            else:
                kw = process_line(line.replace(',',''))
            
            if kw not in fields_list:
                fields_list.append(kw)
    
    logging.info("Success")
    return table_name, fields_list

def process_source(f):
    """
    Extract source table name from the from statement
    """
    _, _, after_bracket = f.partition('{{')
    inside_bracket, _, _ = after_bracket.partition('}}')
    inside_quotes = quoted.findall(inside_bracket)[1]
    return inside_quotes
    
def process_dbt(l):
    """
    Process a line that contains dbt macros with a table field, extract the field
    """
    _, _, after_bracket = l.partition('{{')
    inside_bracket, _, _ = after_bracket.partition('}}')
    inside_quotes = quoted.findall(inside_bracket)[0]
    return inside_quotes

def process_other(l):
    """
    Process a line without dbt macros with a table field, extract the field
    """
    return l.split()[0]

def process_line(l):
    """
    finds and returns a field keyword in a line
    """
#     check if we are dealing with dbt macros
    if dbtline.search(l):
        return process_dbt(l)
    else: 
        return process_other(l)