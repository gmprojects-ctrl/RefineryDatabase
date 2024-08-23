# Import Flask
from flask import Flask,request,jsonify

# Import pandas
import pandas as pd

# Import psycopg2
import psycopg2

# Import util
from utils.refinery_db_io import REFINERY_DB_CONFIG_TEST

# Import get_db_engine
from utils.refinery_db_io import get_db_engine

# Import the class
from utils.refinery_db_io import Refinery

# Import sessionmaker
from sqlalchemy.orm import sessionmaker


# Get the database engine
engine = get_db_engine(REFINERY_DB_CONFIG_TEST)

# Create a new Flask instance
app = Flask(__name__)


# Create a session
Session = sessionmaker(bind=engine)




# Define a get route
@app.route('/')
def index()-> dict:
    '''
    Title: Main route
    Description: This route returns all the data from the refinery table
    Args: None
    Returns: A json object containing the data
    '''
    # Create a session
    with Session() as session:
        try:
            # Get the data
            data = session.query(Refinery).all()
            
            # Convert the data to a string
            data = [ obj.to_dict() for obj in data ]
            
            return jsonify(data), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

# Main filter route
@app.route('/filter', methods=['GET'])  
def filter()-> dict:
    '''
    Title: filter
    Description: This route returns all the data from the refinery table filtered by region, country and status
    Args: region
    Returns: A json object containing the data
    '''
    
    # Get any query parameters
    query_parameters = request.args
    
    
    # Get the region
    region = query_parameters.get('region', None)
    
    # Get the country
    country = query_parameters.get('country', None)
    
    # Get the status
    status = query_parameters.get('status', None)
    
    # Create a session
    with Session() as session:
        try:
            
            # Primary query
            primary_query = session.query(Refinery)

            # If region is not None
            if region:
                primary_query = primary_query.filter(Refinery.region == region)    
                
            
            # If country is not None
            if country:
                primary_query = primary_query.filter(Refinery.country == country)
            
            # If status is not None
            if status:
                primary_query = primary_query.filter(Refinery.status == status)
            
            # Get the data
            data = primary_query.filter(Refinery.region == region).all()
                
            # Convert the data to a string
            data = [ obj.to_dict() for obj in data ]
            
            
            return jsonify(data), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500



if __name__ == "__main__":
    app.run(debug=True)