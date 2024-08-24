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



# Add refinery 
@app.route('/addrefinery', methods=['POST'])
def post_refinery()-> dict:
    '''
    Title: post_refinery
    Description: This route adds a new refinery to the refinery table
    Args: None
    Returns: A json object containing the data
    '''
    # Get the data
    post_data = request.get_json()
    
    # Know the keys
    region = post_data.get('region', 'Unknown')
    country = post_data.get('country', 'Unknown')
    refinery = post_data.get('refinery', 'Unknown')
    capacity = post_data.get('capacity', '0')
    unit = post_data.get('unit', 'kbd')
    status = post_data.get('status', 'closed')
    
    # Create a session
    with Session() as session:
        
        try:
            # Get the maximum id
            max_id = session.query(Refinery).order_by(Refinery.refinery_id.desc()).first().refinery_id
            
            # Ensure max id is a integer
            max_id = int(max_id)
            
            # Increment the id
            max_id += 1
            
            # Create a new refinery object
            new_refinery = Refinery(refinery_id=max_id,region=region, country=country, refinery=refinery, capacity=capacity, unit=unit, status=status)
            
            # Add the refinery
            session.add(new_refinery)
            
            # Commit the session
            session.commit()
            
            return jsonify(new_refinery.to_dict()), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500


# Delete refinery method
@app.route('/deleterefinery/<to_delete_id>', methods=['DELETE'])
def delete_refinery(to_delete_id)-> dict:
    '''
    Title: delete_refinery
    Description: This route deletes a refinery from the refinery table
    Args: to_delete_id
    Returns: A json object containing the data
    '''
    
    # Create a session
    with Session() as session:
        
        try:
            # Get the refinery to delete
            refinery_to_delete = session.query(Refinery).filter(Refinery.refinery_id == to_delete_id).first()
            
            # Delete the refinery
            session.delete(refinery_to_delete)
            
            # Commit the session
            session.commit()
            
            return jsonify(refinery_to_delete.to_dict()), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500


# Update refinery method
@app.route('/updaterefinery/<to_update_id>', methods=['PUT'])
def update_refinery(to_update_id)->None:
    
    # Get the put data
    put_data = request.get_json()
    
    # Get the parameters
    region = put_data.get("region",None)
    country = put_data.get("country",None)
    refinery = put_data.get("refinery",None)
    capacity = put_data.get("capacity",None)
    unit = put_data.get("unit",None)
    status = put_data.get("status",None)
    
    # Create a session
    with Session() as session:
        try:
            # Get the refinery to update
            refinery_to_update = session.query(Refinery).filter(Refinery.refinery_id == to_update_id).first()
            
            # Update the refinery
            if region:
                refinery_to_update.region = region
            
            if country:
                refinery_to_update.country = country
            
            if refinery:
                refinery_to_update.refinery = refinery
            
            if capacity:
                refinery_to_update.capacity = capacity
            
            if unit:
                refinery_to_update.unit = unit
            
            if status:
                refinery_to_update.status = status
                
            # Commit the session
            session.commit()
        
            # Return the updated refinery
            return jsonify(refinery_to_update.to_dict()), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

if __name__ == "__main__":
    app.run(debug=True)