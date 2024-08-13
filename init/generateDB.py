# Import the necessary libraries
import utils.refinery_db_io 


# Import logging
import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
STREAM_HANDLER = logging.StreamHandler()
STREAM_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(STREAM_HANDLER)
FILE_HANDLER = logging.FileHandler("generateDB.log")
FILE_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(FILE_HANDLER)



def main():

    # Test connection to the database
    engine = utils.refinery_db_io.get_db_engine()
    
    
    # Try to create the refinery table
    try:
        utils.refinery_db_io.create_refinery_db(engine)
    except Exception as e:
        LOGGER.error("Error creating refinery table: %s", e)
        raise e
    
    # Log that the refinery table was created
    LOGGER.info("Refinery table created")

    # Test the connection
    try:
        utils.refinery_db_io.test_connection(engine)
        try:
            utils.refinery_db_io.insert_table_into_db(engine)
        except Exception as e:
            LOGGER.error("Error inserting table into database: %s", e)
            raise e

    except Exception as e:
        LOGGER.error("Error testing connection to database: %s", e)
        raise e


    # Dispose of the engine
    engine.dispose()

    
if __name__ == "__main__":
    main()