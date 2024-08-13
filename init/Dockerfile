FROM python:3.12.4-alpine3.20

# Set the working directory
WORKDIR /app

# Copy the requirements file to the container
COPY ./requirements.txt /app/requirements.txt


# Install the dependencies
# Need to install postgresql-libs and postgresql-dev to install psycopg2
RUN apk update
RUN apk add postgresql-dev gcc python3-dev musl-dev
RUN python3 -m pip install -r ./requirements.txt

# Copy the application code to the container
COPY . /app

# Run the application
CMD ["python", "generateDB.py"]
