# Import Flask
from flask import Flask

# Import pandas
import pandas as pd

# Import psycopg2
import psycopg2

# Import util
from init.utils.refinery_db_ext import get_db_engine

# Import quer
from init.utils.refinery_db_ext import execute_query 


# Import test config
from init.utils.refinery_db_ext import REFINERY_DB_CONFIG_TEST

# Get the database engine
engine = get_db_engine(REFINERY_DB_CONFIG_TEST)

# Create a new Flask instance
app = Flask(__name__)




# Define a get route
@app.route('/')
def index():
    try:
        result = execute_query(_engine = engine, _query="SELECT * FROM refinery")
        return  result.to_json(orient='records'), 200
    except Exception as e:
        return str(e), 500



#  Create thee hap 


if __name__ == "__main__":
    app.run(debug=True)