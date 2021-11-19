#!/bin/sh

INSTAL_CONFIG_FILE="./config/install_config.conf"
HAS_S2AGRI_SERVICES=false

function get_install_config_property
{
    grep "^$1=" "${INSTAL_CONFIG_FILE}" | cut -d'=' -f2 | sed -e 's/\r//g'
}

function run_migration_scripts()
{
   local curPath=$1
   local dbName=$2
   #for each sql scripts found in this folder
   for scriptName in "$curPath"/*.sql
   do
        scriptToExecute=${scriptName}
        ## perform execution of each sql script
        echo "Executing SQL script: $scriptToExecute"
        psql -U postgres -f "$scriptToExecute" ${dbName}
   done
}

if [ $# -gt 0 ] ; then
    DB_NAME=$1
    if [ $# -gt 1 ] ; then
        PROFILE=$2
    else 
        PROFILE=$DB_NAME
    fi
else
    DB_NAME=$(get_install_config_property "DB_NAME")
fi

if [ -z "$DB_NAME" ]; then
    DB_NAME="sen2agri"
fi

if [ -z "$PROFILE" ]; then
    PROFILE=$DB_NAME
fi

echo "Using DB = $DB_NAME and profile = $PROFILE"

if [ "$DB_NAME" == "sen2agri" ] ; then 
    cat migrations/migration-1.3-1.3.1.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-1.3.1-1.4.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-1.4-1.5.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-1.5-1.6.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-1.6-1.6.2.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-1.6.2-1.7.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-1.7-1.8.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-1.8.0-1.8.1.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-1.8.1-1.8.2.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-1.8.2-1.8.3.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-1.8.3-2.0.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-2.0.0-2.0.1.sql | su -l postgres -c "psql $DB_NAME"
    cat migrations/migration-2.0.1-2.0.2.sql | su -l postgres -c "psql $DB_NAME"
else 
    run_migration_scripts "migrations/${PROFILE}" "${DB_NAME}"
fi

