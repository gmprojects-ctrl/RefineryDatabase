# Modules
import psycopg2 as pg
from sqlalchemy import create_engine, text
from sqlalchemy import Column, Integer, String, Float   
from sqlalchemy.orm import declarative_base
import pandas as pd
import logging

# Import the get_refinery_data function
from utils.refinery_db_ext import get_refinery_data


# Function to get the database engine

REFINERY_DB_CONFIG = "postgresql://refinery_user:refinery@db:5432/refinery_db"

REFINERY_DB_CONFIG_TEST = "postgresql://refinery_user:refinery@localhost:5432/refinery_db"
    
REFINERY_TABLE_NAME = "refinery"

REFINERY_PRIMARY_KEY = "refinery_id"





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
# Create a db schema
DB_SCHEMA = {
    "refinery_id": Integer,
    "region": String(255),
    "country": String(255),
    "refinery": String(255),
    "capacity": Float,
    "unit": String(255),
    "status": String(255)
}

# Base class
Base = declarative_base()


# Refinery model
class Refinery(Base):
    __tablename__ = REFINERY_TABLE_NAME
    
    refinery_id = Column(REFINERY_PRIMARY_KEY,Integer, primary_key=True, autoincrement=True)
    region = Column('region', String(255), nullable=False)
    country = Column('country', String(255), nullable=False)
    refinery = Column('refinery', String(255), nullable=False)
    capacity = Column('capacity', Float, nullable=False)
    unit = Column('unit', String(255), nullable=False)
    status = Column('status',String(255), nullable=False)

    def __repr__(self):
        # Return the string representation of the object
        return f"<Refinery(id='{self.refinery_id}', region='{self.region}', country='{self.country}', refinery='{self.refinery}', capacity='{self.capacity}', unit='{self.unit}', status='{self.status}')>"
        
    def to_dict(self):
        return {
            'index': self.refinery_id,
            "region": self.region,
            "country": self.country,
            "refinery": self.refinery,
            "capacity": self.capacity,
            "unit": self.unit,
            "status": self.status
        }
def create_refinery_db(engine)->None:
    '''
    Title: create_refinery_db
    Description: This function creates the refinery database.
    Arguments:
        engine: The engine object to connect to the database
    Returns:
        None
    '''
    
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
    
        




def insert_table_into_db(engine,refinery_table_name:str=REFINERY_TABLE_NAME, refinery_schema:dict=DB_SCHEMA)->None:
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
        refinery_data.to_sql(refinery_table_name, con=engine, if_exists='replace', index_label='refinery_id', dtype=refinery_schema)
    
        LOGGER.info("Data inserted into the database")
    except Exception as e:
        LOGGER.error("Error inserting data into the database: %s", e)
        raise e
    

def add_primary_key(engine, table:str =REFINERY_TABLE_NAME, primary_key:str = REFINERY_PRIMARY_KEY, ):
    '''
    Title: make_primary_key
    Description: This function makes the given column the primary key of the table.
    Arguments:
        engine: The engine object to connect to the database
        table: The name of the table
        primary_key: The name of the column to make the primary key
    Returns:
        None
    '''
    
    # Create the query
    with engine.connect() as conn:
        query = text(f"ALTER TABLE {table} ADD PRIMARY KEY({primary_key})")
        
        # Execute the query
        try:
            conn.execute(query)
            LOGGER.info(f"Primary key {primary_key} set for table {table}")
        except Exception as e:
            LOGGER.error(f"Error setting primary key {primary_key} for table {table}: {e}")
            raise e
    
