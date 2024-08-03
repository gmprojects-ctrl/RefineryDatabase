\connect refinery_db;

CREATE TABLE IF NOT EXISTS refinery (
  id SERIAL PRIMARY KEY,
  region VARCHAR(255) NOT NULL,
  country VARCHAR(255) NOT NULL,
  refinery VARCHAR(255) NOT NULL,
  capacity VARCHAR(255) NOT NULL,
  unit VARCHAR(255) NOT NULL,
  status VARCHAR(255) NOT NULL
);
