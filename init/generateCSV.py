import utils.refinery_db_io


# Import logging
import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
STREAM_HANDLER = logging.StreamHandler()
STREAM_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(STREAM_HANDLER)
FILE_HANDLER = logging.FileHandler("generateCSV.log")
FILE_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(FILE_HANDLER)




# Main function
def main():
 
    # Get the refinery data
    LOGGER.info("Getting refinery data")   


    try:
        
        table = utils.refinery_db_io.get_refinery_data()

        # Write the table to a csv file
        table.to_csv("refinery_data.csv", index=False)
    except Exception as e:
        LOGGER.error("Error getting refinery data: %s", e)
        raise e
    
    LOGGER.info("Refinery data written to refinery_data.csv")
    
    
if __name__ == "__main__":
    main()