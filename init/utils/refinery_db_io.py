# Global imports
import requests 
import bs4
import pandas as pd
import psycopg2 as pg
import re
from sqlalchemy import create_engine

# Constants
REFINERY_LINK = "https://en.wikipedia.org/wiki/List_of_oil_refineries"

TONNES_INTO_BBLS = 7.36
ROUND_DIGITS = 1



REFINERY_DB_CONFIG = "postgresql://refinery_user:refinery@db:5432/refinery_db"
    
REFINERY_TABLE_NAME = "refinery"

# Import logging
import logging

# Set up logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGGER_FORMAT = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER_STREAM_HANDLER = logging.StreamHandler()
LOGGER_STREAM_HANDLER.setFormatter(LOGGGER_FORMAT)
LOGGER.addHandler(LOGGER_STREAM_HANDLER)



# Data Manipulation functions
############################################################################################################


# Get refinery data
def get_refinery_data(url=REFINERY_LINK):
    '''
    Title: get_refinery_data
    Description: This function scrapes the refinery data from the given URL.
    Arguments:
        url: URL of the website to scrape the data from
    Returns:
        refinery_data: A pandas DataFrame containing the refinery data
    '''
    
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = bs4.BeautifulSoup(response.content, 'html.parser')
       
        # Extraction flag
        extraction_flag = False
        
        # region_level_header
        region_level_header = ''
        # Country level header
        country_level_header = ''
        # Extracted data
        data = []
        
        # Iterate through the siblings of the european header
        for element in soup.find_all():
            # Country header
            if element.name == "h2":
                if element.text.lower().find("africa") != -1:
                    extraction_flag = True
            
            # If the extraction flag is set to True, extract the data
            if extraction_flag:
                
                # Check if the element is header
                if element.name == "h2":
                    region_level_header = element.text
                    
                # Check if the element is header
                if element.name == "h3":
                    country_level_header = element.text
                    
                # Check if it is a li
                if element.name == "li":
                    data.append([region_level_header, country_level_header, element.text])
            
            
            # If see also is found, break the loop
            if element.name == "h2" and element.text.lower().find("see also") != -1:
                break
        
        # Return the data as a pandas DataFrame
        return format_refinery_table(pd.DataFrame(data, columns=["Region", "Country", "Raw"]))
    else:
        # If the request was not successful, print an error message
        LOGGER.error(f"Failed to get data from URL: {url}")
        raise Exception(f"Failed to get data from URL: {url}")
    
    
# Format refinery data
def format_refinery_table(table:pd.DataFrame)->pd.DataFrame:
    '''
    Title: format_refinery_table
    Description: This function formats the refinery data table.
    Arguments:
        table: A pandas DataFrame containing the refinery data
    Returns:
        formatted_table: A pandas DataFrame containing the formatted refinery data
    '''
    
    # Assumes the columns are Region, Country, Refinery
    
    # Remove [edit] from the Region column
    table['Region'] = table['Region'].str.strip()
    table['Region'] = table['Region'].str.replace("[edit]", "")
    
    # Replace A hat with A
    table['Raw'] = table['Raw'].apply(convert_to_ascii)
    
    # Remove [edit] from the Country column
    table['Country'] = table['Country'].str.strip()
    table['Country'] = table['Country'].str.replace("[edit]", "")
    
    # Get refinery name
    table['Refinery_'] = table['Raw'].str.split(",").str[0]
    
    # Get the refinery name
    table['Refinery'] = table['Refinery_'].str.split("(").str[0]
    
    # If the refinery name is empty, get the name from the refinery_ column
    table['Refinery'] = table['Refinery'].where(table['Refinery'] != "", table['Refinery_'].str.split("(").str[1])
    
    # Strip refinery name of non alphanumeric characters
    table['Refinery'] = table['Refinery'].str.replace(r'\W', ' ')
    
    # Replace brackets with commas
    table['Refinery'] = table['Refinery'].str.replace(')', '')
    
    # Get the capacity
    table['Capacity'] = table['Raw'].apply(extract_bbld)
    
    # Get the capacity
    table['Capacity'] = table['Capacity'].where(table['Capacity'] != 0, table['Raw'].apply(extract_tonnes_to_bbld))
    
    
    
    # Status
    table['Status'] = table['Raw'].apply(checkstatus)
    
    
    # Add unit
    table['Unit'] = "kbd"
    
   # Debugging
   # return table
    
    # Convert columns to lowercase
    table.columns = table.columns.str.lower()
    
    
    # Return the formatted table
    return table[['region', 'country', 'refinery', 'capacity', 'unit', 'status']]

# Extract bbl/day from the raw data
def extract_bbld(text:str)-> int:
    '''
    Title: extract_bbld
    Description: This function extracts the bbl/day from the raw data.
    Arguments:
        text: The raw data
        e.g. Memphis Refinery (Valero), Memphis 180,000Âbbl/d (29,000Âm3/d), -> 180000
    Returns:
        bbl_day: The extracted bbl/day value
    '''
    patterns = [r'(\d{1,3}(?:,\d{3})*) bbl/d',
                r'(\d{1,3}(?:,\d{3})*) bbl/day',
                r'(\d{1,3}(?:,\d{3})*) bp/d',
                r'(\d{1,3}(?:,\d{3})*) bpd',
                r'(\d{1,3}(?:,\d{3})*) barrels/day',
                r'(\d{1,3}(?:,\d{3})*) barrels per day']
   
    for pattern in patterns:
        string_match = re.search(pattern, text)
        if string_match:
            value = string_match.group(1)
            value = value.replace(",", "")
            return int(value) / 1000
    return 0
    
# Extract tonnes to bbl/day
def extract_tonnes_to_bbld(text:str)->int:
    '''
    Title: extract_tonnes_to_bbld
    Description: This function converts the given tonnes to bbl/day.
    Arguments:
        text: The text to convert to bbl/day    
        
    Returns:
        bbl_day: The converted bbl/day value
    
    '''
    
    # Replace the periods with commas
    text = text.replace(".", ",")
    
    # List the patterns
    patterns_tonnes_per_year = [r'(\d{1,3}(?:,\d{3})*) ton/annum', 
                                r'(\d{1,3}(?:,\d{3})*) tonnes/annum', 
                                r'(\d{1,3}(?:,\d{3})*) tonnes/year']
    
    pattern_tonnes_per_day = [r'(\d{1,3}(?:,\d{3})*) ton/day', 
                              r'(\d{1,3}(?:,\d{3})*) tonnes/day', 
                              r'(\d{1,3}(?:,\d{3})*) tonnes/d']
    
    pattern_million_tonnes_per_year = [r'(\d{1,3}(?:.\d{3})*) million tonne/year',
                                       r'(\d{1,3}(?:.\d{3})*) million tonnes/year',
                                        r'(\d{1,3}(?:.\d{3})*) million tonne/annum',
                                        r'(\d{1,3}(?:.\d{3})*) million tonnes/annum',
                                        r'(\d{1,3}(?:.\d{3})*) million tonne per year',
                                        r'(\d{1,3}(?:.\d{3})*) million tonnes per year']
    
    
    # Iterate through the patterns
    for pattern in patterns_tonnes_per_year:
        # Search for the pattern in the text
        string_match = re.search(pattern, text)
        
        # If the pattern is found, extract the value
        if string_match:
            # Extract the value
            value = string_match.group(1)
            
            # Remove the commas
            value = value.replace(",", "")
            
            # Convert the value to an integer
            return round(int(value) * TONNES_INTO_BBLS / (1000*365) , ROUND_DIGITS)
        
    
    # Iterate through the patterns
    for pattern in pattern_tonnes_per_day:
        # Search for the pattern in the text
        string_match = re.search(pattern, text)
        
        # If the pattern is found, extract the value
        if string_match:
            # Extract the value
            value = string_match.group(1)
            
            # Remove the commas
            value = value.replace(",", "")
            
            # Convert the value to an integer
            return round(int(value) * TONNES_INTO_BBLS / 1000, ROUND_DIGITS)
    
    # Iterate through the patterns
    for pattern in pattern_million_tonnes_per_year:
        # Search for the pattern in the text
        string_match = re.search(pattern, text)
        
        # If the pattern is found, extract the value
        if string_match:
            # Extract the value
            value = string_match.group(1)
            
            # Remove the commas
            value = value.replace(",", "")
            
            # Convert the value to an integer
            # Extra 1000 to convert to bbl/day due to matching i.e 3.65 -> 3650
            return round(float(value) * TONNES_INTO_BBLS * 1000000 / (1000*1000*365), ROUND_DIGITS)
    
    return 0
   
# Check the status of the refinery 
def checkstatus(text:str)->str:
    '''
    Title: checkstatus
    Description: This function checks the status of the refinery.
    Arguments:
        text: The text to check the status of the refinery
    Returns:
        status: The status of the refinery
    '''
    
    values_to_find = ["mothballed", "closed", "biorefinery"]
    
    # Check if the text contains the values
    are_values_present = any([text.lower().find(value) != -1 for value in values_to_find])
    
    
    # Check if to be is present
    if text.lower().find("to be") != -1:
        are_values_present = False
    
    if are_values_present:
        return "closed"
    else:
        return "active"
    
# Convert the text to ASCII 
def convert_to_ascii(text:str)->str:
    '''
    Title: convert_to_ascii
    Description: This function converts the given text to ASCII.
    Arguments:
        text: The text to convert to ASCII
    Returns:
        ascii_text: The text converted to ASCII
    '''
    
    return re.sub(r'[^\x00-\x7F]+', ' ', text)


############################################################################################################

# Database functions
############################################################################################################

def get_db_engine(db_config:dict=REFINERY_DB_CONFIG)->pg.extensions.connection:
    '''
    Title: connect_to_db
    Description: This function connects to the database.
    Arguments:
        db_config: A dictionary containing the database configuration
    Returns:
        connection: The connection object to the database
    '''
    
    # Create an engine
    return create_engine(db_config)
 
def test_connection(engine):
    '''
    Title: test_connection
    Description: This function tests the connection to the database.
    Arguments:
        engine: The engine object to connect to the database
    Returns:
        None
    '''
    try:
        # Connect to the database
        with engine.connect() as conn:
            LOGGER.info("Connection to the database successful")
    except Exception as e:
        LOGGER.error("Error connecting to the database: %s", e)
        raise e

def insert_table_into_db(engine,refinery_table_name:str=REFINERY_TABLE_NAME):
    '''
    Title: insert_table_into_db
    Description: This function inserts the table into the database.
    Arguments:
        engine: The engine object to connect to the database
        refinery_table_name: The name of the refinery table
    Returns:
        None
    '''
    
    
    # Get the refinery data
    refinery_data = get_refinery_data()
    
    
    try:
        # Insert the data into the database
        refinery_data.to_sql(refinery_table_name, con=engine, if_exists='replace')
    
        LOGGER.info("Data inserted into the database")
    except Exception as e:
        LOGGER.error("Error inserting data into the database: %s", e)
        raise e
    
    
