# Modules
import psycopg2 as pg
from sqlalchemy import create_engine, text
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
import pandas as pd
import logging

# Import the get_refinery_data function
from utils.refinery_db_ext import get_refinery_data


# Function to get the database engine

REFINERY_DB_CONFIG = "postgresql://refinery_user:refinery@db:5432/refinery_db"

REFINERY_DB_CONFIG_TEST = "postgresql://refinery_user:refinery@localhost:5432/refinery_db"
    
REFINERY_TABLE_NAME = "refinery"






# Create a logger object
LOGGER = logging.getLogger(__name__)
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
FILE_HANDLER = logging.FileHandler('refinery.log')  
FILE_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(FILE_HANDLER)
STREAM_HANDLER = logging.StreamHandler()
STREAM_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(STREAM_HANDLER)
LOGGER.setLevel(logging.INFO)



############################################################################################################
# ORM functions
############################################################################################################


def create_refinery_db(engine, refinery_table_name:str=REFINERY_TABLE_NAME)->None:
    '''
    Title: create_refinery_db
    Description: This function creates the refinery database.
    Arguments:
        engine: The engine object to connect to the database
        refinery_table_name: The name of the refinery table
    Returns:
        None
    '''

    
    # Base class
    Base = declarative_base()


    # Refinery model
    class Refinery(Base):
        __tablename__ = refinery_table_name
        
        id = Column(Integer, primary_key=True, autoincrement=True)
        region = Column(String(255), nullable=False)
        country = Column(String(255), nullable=False)
        refinery = Column(String(255), nullable=False)
        capacity = Column(String(255), nullable=False)
        unit = Column(String(255), nullable=False)
        status = Column(String(255), nullable=False)
    
    


    # Create the table
    try:
        Base.metadata.create_all(engine)
        LOGGER.info("Refinery table created")
    except Exception as e:
        LOGGER.error("Error creating refinery table: %s", e)
        raise e
    
    




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



def execute_query(_engine, _query:str)->pd.DataFrame:
    '''
    Title: execute_query
    Description: This function executes the given query on the database.
    Arguments:
        engine: The engine object to connect to the database
        query: The query to execute
    Returns:
        result: The result of the query
    
    '''

    # Execute the query
    
    try:
        with _engine.connect() as conn:
            result = conn.execute(text(_query))
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as e:
        LOGGER.error("Error executing query: %s", e)
        raise e
    
        




def insert_table_into_db(engine,refinery_table_name:str=REFINERY_TABLE_NAME):
    '''
    Title: insert_table_into_db
    Description: This function inserts the table into the database.
    Arguments:
        engine: The engine object to connect to the database
        refinery_table_name: The name of the refinery table
        refinery_config: The configuration of the refinery database
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
    
    
