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
       LOCALE = 'C.UTF-8'
       TEMPLATE = template0
       CONNECTION LIMIT = -1;

