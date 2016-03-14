-- Role: admin
CREATE ROLE admin WITH LOGIN PASSWORD 'sen2agri'
  SUPERUSER;

-- Role: sen2agri-service
CREATE ROLE "sen2agri-service" WITH LOGIN PASSWORD 'sen2agri';

-- DataBase Create: sen2agri
CREATE DATABASE sen2agri
  WITH OWNER = postgres
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       LC_COLLATE = 'en_US.UTF-8'
       LC_CTYPE = 'en_US.UTF-8'
       CONNECTION LIMIT = -1;

-- Privileges: sen2agri
GRANT ALL PRIVILEGES ON DATABASE sen2agri TO admin;

-- Privileges: sen2agri
GRANT ALL PRIVILEGES ON DATABASE sen2agri TO "sen2agri-service";

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA public 
TO "sen2agri-service";