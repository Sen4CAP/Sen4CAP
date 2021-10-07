#!/bin/sh

INSTAL_CONFIG_FILE="./config/install_config.conf"
HAS_S2AGRI_SERVICES=false

: ${GPT_CONFIG_FILE:="./config/gpt.vmoptions"}
: ${SYS_ACC_NAME:="sen2agri-service"}
: ${SLURM_QOS_LIST:="qosMaccs,qosComposite,qosCropMask,qosCropType,qosPheno,qosLai,qoss4cmdb1,qoss4cl4a,qoss4cl4b,qoss4cl4c"}


function get_install_config_property
{
    grep "^$1=" "${INSTAL_CONFIG_FILE}" | cut -d'=' -f2 | sed -e 's/\r//g'
}

function install_sen2agri_services()
{
    SERVICES_ARCHIVE=$(get_install_config_property "SERVICES_ARCHIVE")
    if [ -z "$SERVICES_ARCHIVE" ]; then
        if [ -f ../sen2agri-services/sen2agri-services*.zip ]; then
            zipArchive=$(ls -at ../sen2agri-services/sen2agri-services*.zip| head -n 1)
        fi
    else
        if [ -f "../sen2agri-services/${SERVICES_ARCHIVE}" ]; then
            zipArchive=$(ls -at "../sen2agri-services/${SERVICES_ARCHIVE}" | head -n 1)
        fi
    fi

    # Check if directory does not exists or is empty
    if [ ! -d "${TARGET_SERVICES_DIR}" ] || [ ! "$(ls -A ${TARGET_SERVICES_DIR})" ] ; then
        if [ -f ../sen2agri-services/${SERVICES_ARCHIVE} ]; then
            echo "Extracting into ${TARGET_SERVICES_DIR} from archive $zipArchive ..."

            mkdir -p ${TARGET_SERVICES_DIR} && unzip ${zipArchive} -d ${TARGET_SERVICES_DIR}
            if [ $? -ne 0 ]; then
                echo "Unable to unpack the sen2agri-services into ${TARGET_SERVICES_DIR}"
                echo "Exiting now"
                exit 1
            fi
            # convert any possible CRLF into LF
            tr -d '\r' < ${TARGET_SERVICES_DIR}/bin/start.sh > ${TARGET_SERVICES_DIR}/bin/start.sh.tmp && cp -f ${TARGET_SERVICES_DIR}/bin/start.sh.tmp ${TARGET_SERVICES_DIR}/bin/start.sh && rm ${TARGET_SERVICES_DIR}/bin/start.sh.tmp
            # ensure the execution flag
            chmod a+x ${TARGET_SERVICES_DIR}/bin/start.sh
        else
            echo "No sen2agri-services zip archive provided in ../sen2agri-services"
            echo "Exiting now"
            exit 1
        fi
    else
        echo "sen2agri-services already exist in ${TARGET_SERVICES_DIR}"
        if [ -d "${TARGET_SERVICES_DIR}/bin" ] && [ -d "${TARGET_SERVICES_DIR}/config" ] ; then

            add_plgs_bkp=lib_add_plgs_bkp_$( date "+%Y_%m_%d_%H_%M_%S" )
            #check if lib directory exist and is not empty
            if [ -d "${TARGET_SERVICES_DIR}/lib" ] && [ ! -z "$(ls -A ${TARGET_SERVICES_DIR}/lib)" ] ; then
                mkdir ${TARGET_SERVICES_DIR}/$add_plgs_bkp
                for filepath in ${TARGET_SERVICES_DIR}/lib/tao-datasources-*.jar
                do
                    filename=$(basename $filepath)
                    #make a backup for tao-datasource*.jar, others that scihub and usgs
                    if [[ $filename != tao-datasources-scihub* ]] && [[ $filename != tao-datasources-usgs* ]]; then
                        cp ${TARGET_SERVICES_DIR}/lib/$filename ${TARGET_SERVICES_DIR}/$add_plgs_bkp/
                    fi
                done;
            fi
            if [ -f ../sen2agri-services/${SERVICES_ARCHIVE} ]; then
                echo "Updating ${TARGET_SERVICES_DIR}/lib folder ..."
                mkdir -p ${TARGET_SERVICES_DIR}/lib && rm -f ${TARGET_SERVICES_DIR}/lib/*.jar && unzip -o ${zipArchive} 'lib/*' -d ${TARGET_SERVICES_DIR}
                # Check if directory lib_add_plgs_bkp_<timestamp> exist and is not empty
                if [ -d "${TARGET_SERVICES_DIR}/${add_plgs_bkp}" ] ; then
                    if [ ! -z "$(ls -A ${TARGET_SERVICES_DIR}/${add_plgs_bkp})" ]; then
                        for filepath in ${TARGET_SERVICES_DIR}/$add_plgs_bkp/tao-datasources-*.jar
                        do
                            filename=$(basename $filepath| grep -oP '.*(?=-)')
                            if [ -f ../sen2agri-services/datasource-additional-plugins/$filename*.jar ];then
                                cp ../sen2agri-services/datasource-additional-plugins/$filename*.jar ${TARGET_SERVICES_DIR}/lib/
                            else
                                echo "IT WAS USED THE VERSION FOUND IN LIB FOLDER OF " $filename " BUT MAY NOT BE COMPATIBLE WITH CURRENT VERSION OF SEN2AGRI-SERVICES  "
                                cp ${TARGET_SERVICES_DIR}/$add_plgs_bkp/$filename*.jar ${TARGET_SERVICES_DIR}/lib/
                            fi
                        done;
                    fi
                    rm -rf ${TARGET_SERVICES_DIR}/$add_plgs_bkp
                fi
                echo "Updating ${TARGET_SERVICES_DIR}/modules folder ..."
                mkdir -p ${TARGET_SERVICES_DIR}/modules && rm -f ${TARGET_SERVICES_DIR}/modules/*.jar && unzip -o ${zipArchive} 'modules/*' -d ${TARGET_SERVICES_DIR}

                echo "Updating ${TARGET_SERVICES_DIR}/static folder ..."
                mkdir -p ${TARGET_SERVICES_DIR}/static && rm -fR ${TARGET_SERVICES_DIR}/static/* && unzip -o ${zipArchive} 'static/*' -d ${TARGET_SERVICES_DIR}

                mkdir -p ${TARGET_SERVICES_DIR}/scripts && rm -fR ${TARGET_SERVICES_DIR}/scripts/* && unzip -o ${zipArchive} 'scripts/*' -d ${TARGET_SERVICES_DIR}

                if [ -f ${TARGET_SERVICES_DIR}/config/sen2agri-services.properties ] ; then
                    mv ${TARGET_SERVICES_DIR}/config/sen2agri-services.properties ${TARGET_SERVICES_DIR}/config/services.properties
                fi
                if [ -f ${TARGET_SERVICES_DIR}/config/application.properties ] ; then
                    cp -f ${TARGET_SERVICES_DIR}/config/application.properties ${TARGET_SERVICES_DIR}/config/application.properties.bkp
                fi
                # update the application.properties file even if some user changes might be lost
                unzip -o ${zipArchive} 'config/application.properties' -d ${TARGET_SERVICES_DIR}/
            else
                echo "No archive sen2agri-services-YYY.zip was found in the installation package. sen2agri-services will not be updated!!!"
            fi
        else
            echo "ERROR: no bin or config folder were found in the folder ${TARGET_SERVICES_DIR}/. No update will be made!!!"
        fi
        HAS_S2AGRI_SERVICES=true
    fi
    # it might happen that some files to be packaged with the wrong read rights
    chmod -R a+r ${TARGET_SERVICES_DIR}
}

SCIHUB_USER=""
SCIHUB_PASS=""
USGS_USER=""
USGS_PASS=""
function saveOldDownloadCredentials()
{
    if [ -f /usr/share/sen2agri/sen2agri-downloaders/apihub.txt ]; then
        apihubLine=($(head -n 1 /usr/share/sen2agri/sen2agri-downloaders/apihub.txt))
        SCIHUB_USER=${apihubLine[0]}
        SCIHUB_PASS=${apihubLine[1]}
    fi
    if [ -f /usr/share/sen2agri/sen2agri-downloaders/usgs.txt ]; then
        usgsLine=($(head -n 1 /usr/share/sen2agri/sen2agri-downloaders/usgs.txt))
        USGS_USER=${usgsLine[0]}
        USGS_PASS=${usgsLine[1]}
    fi
}

function updateDownloadCredentials()
{
    if [ "$HAS_S2AGRI_SERVICES" = false ] ; then
        sed -i '/SciHubDataSource.username=/c\SciHubDataSource.username='"$SCIHUB_USER" ${TARGET_SERVICES_DIR}/config/services.properties
        sed -i '/SciHubDataSource.password=/c\SciHubDataSource.password='"$SCIHUB_PASS" ${TARGET_SERVICES_DIR}/config/services.properties
        sed -i '/USGSDataSource.username=/c\USGSDataSource.username='"$USGS_USER" ${TARGET_SERVICES_DIR}/config/services.properties
        sed -i '/USGSDataSource.password=/c\USGSDataSource.password='"$USGS_PASS" ${TARGET_SERVICES_DIR}/config/services.properties
    fi
}

function enableSciHubDwnDS()
{
    echo "Disabling Amazon datasource ..."
    sed -i '/SciHubDataSource.Sentinel2.scope=1/c\SciHubDataSource.Sentinel2.scope=3' ${TARGET_SERVICES_DIR}/config/services.properties
    sed -i '/AWSDataSource.Sentinel2.enabled=true/c\AWSDataSource.Sentinel2.enabled=false' ${TARGET_SERVICES_DIR}/config/services.properties

    sed -i 's/AWSDataSource.Sentinel2.local_archive_path=/SciHubDataSource.Sentinel2.local_archive_path=/g' ${TARGET_SERVICES_DIR}/config/services.properties
    sed -i 's/AWSDataSource.Sentinel2.fetch_mode=/SciHubDataSource.Sentinel2.fetch_mode=/g' ${TARGET_SERVICES_DIR}/config/services.properties

    psql -U postgres $DB_NAME -c "update datasource set scope = 3 where satellite_id = 1 and name = 'Scientific Data Hub';"
    psql -U postgres $DB_NAME -c "update datasource set enabled = 'false' where satellite_id = 1 and name = 'Amazon Web Services';"
    echo "Disabling Amazon datasource ... Done!"

#    sudo -u postgres psql sen2agri -c "update datasource set local_root = (select local_root from datasource where satellite_id = 1 and name = 'Amazon Web Services') where satellite_id = 1 and name = 'Scientific Data Hub';"
#    sudo -u postgres psql sen2agri -c "update datasource set fetch_mode = (select fetch_mode from datasource where satellite_id = 1 and name = 'Amazon Web Services') where satellite_id = 1 and name = 'Scientific Data Hub';"
}

function updateWebConfigParams()
{
    # Set the port 8082 for the dashboard services URL
    sed -i -e "s|static \$DEFAULT_SERVICES_URL = \x27http:\/\/localhost:8080\/dashboard|static \$DEFAULT_SERVICES_URL = \x27http:\/\/localhost:8082\/dashboard|g" /var/www/html/ConfigParams.php
    sed -i -e "s|static \$DEFAULT_SERVICES_URL = \x27http:\/\/localhost:8081\/dashboard|static \$DEFAULT_SERVICES_URL = \x27http:\/\/localhost:8082\/dashboard|g" /var/www/html/ConfigParams.php

    REST_SERVER_PORT=$(sed -n 's/^server.port =//p' ${TARGET_SERVICES_DIR}/config/services.properties | sed -e 's/\r//g')
    # Strip leading space.
    REST_SERVER_PORT="${REST_SERVER_PORT## }"
    # Strip trailing space.
    REST_SERVER_PORT="${REST_SERVER_PORT%% }"
     if [[ !  -z  $REST_SERVER_PORT  ]] ; then
        sed -i -e "s|static \$DEFAULT_REST_SERVICES_URL = \x27http:\/\/localhost:8080|static \$DEFAULT_REST_SERVICES_URL = \x27http:\/\/localhost:$REST_SERVER_PORT|g" /var/www/html/ConfigParams.php
     fi

    if [[ ! -z $DB_NAME ]] ; then
        sed -i -e "s|static \$DEFAULT_DB_NAME = \x27sen2agri|static \$DEFAULT_DB_NAME = \x27${DB_NAME}|g" /var/www/html/ConfigParams.php
    fi

}

function resetDownloadFailedProducts()
{
    echo "Resetting failed downloaded products from downloader_history ..."
    psql -U postgres $DB_NAME -c "update downloader_history set no_of_retries = '0' where status_id = '3' "
    psql -U postgres $DB_NAME -c "update downloader_history set no_of_retries = '0' where status_id = '4' "
    psql -U postgres $DB_NAME -c "update downloader_history set status_id = '3' where status_id = '4' "
    echo "Resetting failed downloaded products from downloader_history ... Done!"
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

function install_docker() {
    systemctl -q is-enabled docker
    if [ $? -ne 0 ]; then
        echo "Installing docker"
        yum -y update epel-release
        yum -y install docker
        sed -i "s/'--selinux-enabled /'/" /etc/sysconfig/docker
        systemctl enable docker
    fi

    yum -y install jq docker-compose
    jq '. + { group: "dockerroot" }' < /etc/docker/daemon.json > /etc/docker/daemon.json.new
    mv -f /etc/docker/daemon.json.new /etc/docker/daemon.json
    usermod -aG dockerroot ${SYS_ACC_NAME}

    systemctl restart docker
}

function migrate_postgres_to_docker() {
    systemctl -q is-enabled postgresql-9.4
    if [ $? -ne 0 ]; then
        return
    fi

    echo "Backing up database"
    sudo -u postgres pg_dumpall > /tmp/db.sql

    echo "Stopping old Postgres"
    systemctl stop postgresql-9.4
    systemctl disable postgresql-9.4

    echo "Installing yum-utils"
    yum -y install yum-utils

    echo "Uninstalling PGDG packages"
    yum -y autoremove $(yumdb search from_repo pgdg94 | awk -F"\n" '{ RS=""; print $1 }' | grep -v subscription-manager)

    echo "Removing old PGDG repository"
    yum -y autoremove pgdg-centos94

    echo "Uninstalling old packages"
    yum -y autoremove otb sen2agri-processors gdal-libs gdal-python geos geos38 libgeotiff proj49 python2-psycopg2

    echo "Installing Postgres client libraries and tools"
    yum -y install https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
    yum -y update pgdg-redhat-repo
    yum-config-manager --disable pgdg95
    yum -y install postgresql12 python-psycopg2 gdal-python

    echo "Starting Postgres container"
    cd docker
    docker-compose up -d db

    RETRIES=120
    until docker-compose exec db pg_isready || [ $RETRIES -eq 0 ]; do
        echo "Waiting for postgres, $RETRIES remaining attempts..."
        RETRIES=$((RETRIES-1))
        sleep 1
    done

    echo "Waiting 120 seconds for postgres to settle..."
    sleep 120

    RETRIES=120
    until docker-compose exec db pg_isready || [ $RETRIES -eq 0 ]; do
        echo "Waiting for postgres, $RETRIES remaining attempts..."
        RETRIES=$((RETRIES-1))
        sleep 1
    done

    cd ..

    echo "Restoring database backup"
    psql -U postgres -f /tmp/db.sql
}

function setup_containers() {
    cd docker
    docker-compose up -d
    cd ..

    docker pull osgeo/gdal:ubuntu-full-3.2.0
    docker pull sen4x/fmask_extractor:0.1
    docker pull sen4x/fmask:4.2

    docker pull sen4cap/processors:2.0.0
    docker pull sen4cap/grassland_mowing:2.0.0
    docker pull sen4x/l2a-processors:0.1
    docker pull sen4x/sen2cor:2.9.0-ubuntu-20.04
    docker pull sen4x/maja:3.2.2-centos-7
    docker pull sen4x/l2a-l8-alignment:0.1
    docker pull sen4x/l2a-dem:0.1
}

function migrate_to_docker() {
    install_docker
    migrate_postgres_to_docker
    setup_containers
}

# TODO: This function is the same as the one in the installation script. Should be extracted in a common functions file
function create_and_config_slurm_qos()
{
   #extract each configured QOS from SLURM_QOS_LIST
   IFS=',' read -ra ADDR <<< "${SLURM_QOS_LIST}"

   #for each qos defined in configuration, add the missing QOS
   for qosName in "${ADDR[@]}"; do
        if [ -z $(sacctmgr list qos --parsable | grep -i ${qosName}) ] ; then
            #add qos to slurm
            SLURM_ADD_QOS=$(expect -c "
             set timeout 5
             spawn sacctmgr add qos  \"${qosName}\"
             expect \"Would you like to commit changes? (You have 30 seconds to decide)\"
             send \"y\r\"
             expect eof
            ")
            echo "$SLURM_ADD_QOS"

            #set qos number of jobs able to run at any given time
            SLURM_JOBS_PER_QOS=$(expect -c "
             set timeout 5
             spawn sacctmgr modify qos "${qosName}" set GrpJobs=1
             expect \"Would you like to commit changes? (You have 30 seconds to decide)\"
             send \"y\r\"
             expect eof
            ")
            echo "$SLURM_JOBS_PER_QOS"

            #add already created qos to user , and another qos if that qos already exists
            SLURM_ADD_QOS_TO_ACC=$(expect -c "
             set timeout 5
             spawn sacctmgr modify user "${SYS_ACC_NAME}" set qos+="${qosName}"
             expect \"Would you like to commit changes? (You have 30 seconds to decide)\"
             send \"y\r\"
             expect eof
            ")
            echo "$SLURM_ADD_QOS_TO_ACC"
        fi
   done

   #show current configuration for SLURM
   echo "CLUSTER,USERS,QOS INFO:"
   sacctmgr show assoc format=cluster,user,qos

   echo "QOS INFO:"
   sacctmgr list qos

   echo "Partition INFO:"
   scontrol show partition

   echo "Nodes INFO:"
   scontrol show node
}

function update_maja_gipp() {
    VAL=$(psql -qtAX -U admin ${DB_NAME} -c "select value from config where key = 'processor.l2a.maja.gipp-path' and site_id is null")
    if [ ! -z $VAL ] ; then
        if [ -d $VAL ] ; then
            echo "Key processor.l2a.maja.gipp-path found with value ${VAL}. Copying UserConfiguration into this location ..."
            cp -fR ./config/maja/UserConfiguration ${VAL}
        else
            echo "WARNING: Key processor.l2a.maja.gipp-path found in config table for database ${DB_NAME} with value $VAL but the directory does not exists for this value. UserConfiguration not updated ..."
        fi
    else
        echo "WARNING: Key processor.l2a.maja.gipp-path not found in config table for database ${DB_NAME}. UserConfiguration not updated ..."
    fi
}

systemctl stop sen2agri-scheduler sen2agri-executor sen2agri-orchestrator sen2agri-http-listener sen2agri-sentinel-downloader sen2agri-landsat-downloader sen2agri-demmaccs sen2agri-sentinel-downloader.timer sen2agri-landsat-downloader.timer sen2agri-demmaccs.timer sen2agri-monitor-agent sen2agri-services

saveOldDownloadCredentials
migrate_to_docker

yum -y install python-dateutil libcurl-devel openssl-devel libxml2-devel php-pgsql
yum -y install ../rpm_binaries/*.rpm
rm -rf /usr/local/bin/gdal_edit.py

DB_NAME=$(get_install_config_property "DB_NAME")
if [ -z "$DB_NAME" ]; then
    DB_NAME="sen2agri"
fi

echo "$DB_NAME"

TARGET_SERVICES_DIR="/usr/share/sen2agri/sen2agri-services"
#if [ "$DB_NAME" != "sen2agri" ] ; then
#    if [ -d "/usr/share/sen2agri/${DB_NAME}-services" ] ; then
#        TARGET_SERVICES_DIR="/usr/share/sen2agri/${DB_NAME}-services"
#    fi
#fi

install_sen2agri_services

create_and_config_slurm_qos

ldconfig

if [ "$DB_NAME" == "sen2agri" ] ; then
    psql -U postgres -f migrations/migration-1.3-1.3.1.sql $DB_NAME
    psql -U postgres -f migrations/migration-1.3.1-1.4.sql $DB_NAME
    psql -U postgres -f migrations/migration-1.4-1.5.sql $DB_NAME
    psql -U postgres -f migrations/migration-1.5-1.6.sql $DB_NAME
    psql -U postgres -f migrations/migration-1.6-1.6.2.sql $DB_NAME
    psql -U postgres -f migrations/migration-1.6.2-1.7.sql $DB_NAME
    psql -U postgres -f migrations/migration-1.7-1.8.sql $DB_NAME
    psql -U postgres -f migrations/migration-1.8.0-1.8.1.sql $DB_NAME
    psql -U postgres -f migrations/migration-1.8.1-1.8.2.sql $DB_NAME
    psql -U postgres -f migrations/migration-1.8.2-1.8.3.sql $DB_NAME
    psql -U postgres -f migrations/migration-1.8.3-2.0.sql $DB_NAME
    psql -U postgres -f migrations/migration-2.0.0-2.0.1.sql $DB_NAME
    psql -U postgres -f migrations/migration-2.0.1-2.0.2.sql $DB_NAME
    psql -U postgres -f migrations/migration-2.0.2-2.0.3.sql $DB_NAME
    psql -U postgres -f migrations/migration-2.0.2-2.0.3-reports.sql $DB_NAME
else
    run_migration_scripts "migrations/${DB_NAME}" "${DB_NAME}"
fi

update_maja_gipp

systemctl daemon-reload
systemctl restart httpd

mkdir -p /mnt/archive/reference_data
echo "Copying reference data"
if [ -d ../reference_data/ ]; then
    cp -rf ../reference_data/* /mnt/archive/reference_data
fi

# Update the port in /var/www/html/ConfigParams.php as version 1.8 had 8080 instead of 8081
updateWebConfigParams

if [ "$DB_NAME" == "sen2agri" ] ; then
    updateDownloadCredentials

    # Enable SciHub as the download datasource
    enableSciHubDwnDS

    # Reset the download failed products
    resetDownloadFailedProducts
else
    # Install and config SNAP
    # check if docker image already exists
    # TODO: "docker image inspect sen4cap/snap" might be also used instead images -q
    #if [[ "$(docker images -q sen4cap/snap 2> /dev/null)" == "" ]]; then
        wget http://step.esa.int/downloads/8.0/installers/esa-snap_sentinel_unix_8_0.sh && \
        mv -f esa-snap_sentinel_unix_8_0.sh ./docker/snap8/ && \
        chmod +x ./docker/snap8/esa-snap_sentinel_unix_8_0.sh && \
        docker build -t sen4cap/snap:8.0 -f ./docker/snap8/Dockerfile ./docker/snap8/
    #else 
    #    echo "No need to install SNAP container, it already exists ..."
    #fi
fi

if [ ! -d /var/log/sen2agri ]; then
    mkdir -p /var/log/sen2agri
    chown sen2agri-service: /var/log/sen2agri
fi

systemctl start sen2agri-executor sen2agri-orchestrator sen2agri-http-listener sen2agri-demmaccs sen2agri-demmaccs.timer sen2agri-monitor-agent sen2agri-scheduler sen2agri-services


