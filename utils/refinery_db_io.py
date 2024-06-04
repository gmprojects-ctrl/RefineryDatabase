# Global imports
import requests 
import bs4
import pandas as pd
import re


# Constants
REFINERY_LINK = "https://en.wikipedia.org/wiki/List_of_oil_refineries"

TONNES_INTO_BBLS = 7.36
ROUND_DIGITS = 1


# Import logging
import logging

# Set up logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGGER_FORMAT = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER_STREAM_HANDLER = logging.StreamHandler()
LOGGER_STREAM_HANDLER.setFormatter(LOGGGER_FORMAT)
LOGGER.addHandler(LOGGER_STREAM_HANDLER)


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
    
    # Return the formatted table
    return table[['Region', 'Country', 'Refinery', 'Capacity', 'Unit', 'Status']]

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
    
   
    pattern = r'(\d{1,3}(?:,\d{3})*) bbl/d'
    pattern_2 = r'(\d{1,3}(?:,\d{3})*) bpd'

    # Search for the pattern in the text
    string_match = re.search(pattern, text)
    
    # If the pattern is not found, search for the second pattern
    if not string_match:
        string_match = re.search(pattern_2, text)
    
    
    # If the pattern is found, extract the value
    if string_match:
        # Extract the value
        value = string_match.group(1)
        
        # Remove the commas
        value = value.replace(",", "")
        
        # Convert the value to an integer
        return int(value) / 1000
        
    else:
        return 0
    

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
    patterns_tonnes_per_year = [r'(\d{1,3}(?:,\d{3})*) ton/annum', r'(\d{1,3}(?:,\d{3})*) tonnes/annum', r'(\d{1,3}(?:,\d{3})*) tonnes/year']
    
    pattern_tonnes_per_day = [r'(\d{1,3}(?:,\d{3})*) ton/day', r'(\d{1,3}(?:,\d{3})*) tonnes/day', r'(\d{1,3}(?:,\d{3})*) tonnes/d']
    
    
    
    
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
    
    
    
    return 0
   
   
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