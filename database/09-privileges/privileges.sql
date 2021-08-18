-- Privileges: sen2agri
GRANT ALL PRIVILEGES ON DATABASE sen2agri TO admin;

-- Privileges: sen2agri
GRANT ALL PRIVILEGES ON DATABASE sen2agri TO "sen2agri-service";

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA public
TO admin;

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA public
TO "sen2agri-service";

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA public
TO admin;

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA public
TO "sen2agri-service";

alter default privileges in schema public grant all on sequences to admin;
alter default privileges in schema public grant all on sequences to "sen2agri-service";

alter default privileges in schema public grant all on tables to admin;
alter default privileges in schema public grant all on tables to "sen2agri-service";
