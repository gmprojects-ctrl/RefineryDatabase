FROM postgres:16.3-alpine3.20

# Set the environment variables
ENV POSTGRES_DB=refinery_db
ENV POSTGRES_USER=refinery_user
ENV POSTGRES_PASSWORD=refinery



# Copy the SQL scripts to the container (Deprecated)
# COPY ./init.sql /docker-entrypoint-initdb.d/

# Expose the port
EXPOSE 5432