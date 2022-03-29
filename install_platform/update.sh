#!/bin/sh

INSTAL_CONFIG_FILE="./config/install_config.conf"
HAS_S2AGRI_SERVICES=false

: ${GPT_CONFIG_FILE:="./config/gpt.vmoptions"}
: ${SYS_ACC_NAME:="sen2agri-service"}
: ${SLURM_QOS_LIST:="qosMaccs,qosComposite,qosCropMask,qosCropType,qosPheno,qosLai,qoss4cmdb1,qoss4cl4a,qoss4cl4b,qoss4cl4c,qosfmask,qosvaliditymsk,qostrex"}


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
            datasources_plugins_dir=""
            if [ -d "${TARGET_SERVICES_DIR}/datasources" ] && [ ! -z "$(ls -A ${TARGET_SERVICES_DIR}/datasources)" ] ; then
                datasources_plugins_dir="${TARGET_SERVICES_DIR}/datasources"
            else
            if [ -d "${TARGET_SERVICES_DIR}/lib" ] && [ ! -z "$(ls -A ${TARGET_SERVICES_DIR}/lib)" ] ; then
                    datasources_plugins_dir="${TARGET_SERVICES_DIR}/lib"
                fi
            fi            
            #check if lib directory exist and is not empty
            if [ -d "${datasources_plugins_dir}" ] && [ ! -z "$(ls -A ${datasources_plugins_dir})" ] ; then
                mkdir -p ${TARGET_SERVICES_DIR}/$add_plgs_bkp
                for filepath in ${datasources_plugins_dir}/tao-datasources-*.jar
                do
                    filename=$(basename $filepath)
                    #make a backup for tao-datasource*.jar
                    cp ${datasources_plugins_dir}/$filename ${TARGET_SERVICES_DIR}/$add_plgs_bkp/
                done;
            fi
            if [ -f ../sen2agri-services/${SERVICES_ARCHIVE} ]; then
                echo "Updating ${TARGET_SERVICES_DIR}/datasources folder ..."
                mkdir -p ${TARGET_SERVICES_DIR}/datasources && rm -f ${TARGET_SERVICES_DIR}/datasources/*.jar && unzip -o ${zipArchive} 'datasources/*' -d ${TARGET_SERVICES_DIR}
                # Check if directory lib_add_plgs_bkp_<timestamp> exist and is not empty
                if [ -d "${TARGET_SERVICES_DIR}/${add_plgs_bkp}" ] ; then
                    if [ ! -z "$(ls -A ${TARGET_SERVICES_DIR}/${add_plgs_bkp})" ]; then
                        for filepath in ${TARGET_SERVICES_DIR}/$add_plgs_bkp/tao-datasources-*.jar
                        do
                            filename=$(basename $filepath| grep -oP '.*(?=-)')
                            if [ -f ../sen2agri-services/datasource-additional-plugins/$filename*.jar ];then
                                echo "Copying file ../sen2agri-services/datasource-additional-plugins/$filename*.jar to ${TARGET_SERVICES_DIR}/datasource/ folder ..."
                                cp -fr ../sen2agri-services/datasource-additional-plugins/$filename*.jar ${TARGET_SERVICES_DIR}/datasources/
                            else
                                echo "IT WAS USED THE VERSION FOUND IN LIB FOLDER OF " $filename " BUT MAY NOT BE COMPATIBLE WITH CURRENT VERSION OF SEN2AGRI-SERVICES  "
                                cp -fr ${TARGET_SERVICES_DIR}/$add_plgs_bkp/$filename*.jar ${TARGET_SERVICES_DIR}/datasources/
                            fi
                        done;
                    fi
                    if [ -d ${TARGET_SERVICES_DIR}/$add_plgs_bkp ]; then 
                        echo "Removing directory ${TARGET_SERVICES_DIR}/$add_plgs_bkp"
                        rm -rf ${TARGET_SERVICES_DIR}/$add_plgs_bkp
                    fi
                fi

                echo "Updating ${TARGET_SERVICES_DIR}/lib folder ..."
                mkdir -p ${TARGET_SERVICES_DIR}/lib && rm -f ${TARGET_SERVICES_DIR}/lib/*.jar && unzip -o ${zipArchive} 'lib/*' -d ${TARGET_SERVICES_DIR}

                echo "Updating ${TARGET_SERVICES_DIR}/modules folder ..."
                mkdir -p ${TARGET_SERVICES_DIR}/modules && rm -f ${TARGET_SERVICES_DIR}/modules/*.jar && unzip -o ${zipArchive} 'modules/*' -d ${TARGET_SERVICES_DIR}

                echo "Updating ${TARGET_SERVICES_DIR}/static folder ..."
                mkdir -p ${TARGET_SERVICES_DIR}/static && rm -fR ${TARGET_SERVICES_DIR}/static/* && unzip -o ${zipArchive} 'static/*' -d ${TARGET_SERVICES_DIR}

                mkdir -p ${TARGET_SERVICES_DIR}/scripts && rm -fR ${TARGET_SERVICES_DIR}/scripts/* && unzip -o ${zipArchive} 'scripts/*' -d ${TARGET_SERVICES_DIR}

                if [ -f ${TARGET_SERVICES_DIR}/config/sen2agri-services.properties ] ; then
                    mv ${TARGET_SERVICES_DIR}/config/sen2agri-services.properties ${TARGET_SERVICES_DIR}/config/services.properties
                fi
                
                
                if grep -q "'../modules/\*:../lib/\*:../services/\*:../plugins/\*'" ${TARGET_SERVICES_DIR}/bin/start.sh
                then
                    echo "start.sh corresponds does not have datasources directory included. Added datasources to classpath ..."
                    sed -i "s/plugins\/\\*/plugins\/\\*\:\.\.\/datasources\/\\*/g" ${TARGET_SERVICES_DIR}/bin/start.sh
                else
                    if grep -q "../datasources/\*" ${TARGET_SERVICES_DIR}/bin/start.sh
                    then
                        echo "start.sh already have datasources directory added in classpath. Nothing to do ..."
                    else
                        echo "Cannot identify the classpath line in services start.sh ... "
                    fi
                fi
                
                # Add new lines for 3.0 if missing
                if grep -q "endpoints.not.authenticated" ${TARGET_SERVICES_DIR}/config/services.properties
                then
                    echo "File services.properties correspond to version 3.0 or later. Updating the endpoints.not.authenticated list ..."
                    sed -i 's/endpoints.not.authenticated=.*/endpoints.not.authenticated=\/;\/login;\/products\/download;\/users\/pwd\/request;\/users\/pwd\/reset/g' ${TARGET_SERVICES_DIR}/config/services.properties

                else
                    echo "Updating 3.0 site infos ..."
                    sed -i '/^plugins.use.docker =.*/i site.location=static\r\nvector.tile.service.url = http:\/\/localhost:6767\r\nsite.prefix = \/ui\r\nendpoints.not.authenticated=\/;\/login;\/products\/download;\/users\/pwd\/request;\/users\/pwd\/reset\r\n\r\n' ${TARGET_SERVICES_DIR}/config/services.properties
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
    chown -R ${SYS_ACC_NAME}: ${TARGET_SERVICES_DIR}/static/
    
    # cleanup the /home/sen2agri-service/.snap and /home/sen2agri-service/.sen2agri-services in case the gdal version changed
    if [ -d /home/${SYS_ACC_NAME}/.snap ] ; then
        rm -fr /home/${SYS_ACC_NAME}/.snap
    fi
    if [ -d /home/${SYS_ACC_NAME}/.sen2agri-services ] ; then
        rm -fr /home/${SYS_ACC_NAME}/.sen2agri-services
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
    docker pull osgeo/gdal:ubuntu-full-3.2.0
    docker pull sen4x/fmask_extractor:0.1
    docker pull sen4x/fmask:4.2

    docker pull sen4cap/processors:3.0.0
    docker pull sen4cap/data-preparation:0.1
    docker pull sen4cap/grassland_mowing:3.0.0
    docker pull sen4x/l2a-processors:0.1
    docker pull sen4x/sen2cor:2.9.0-ubuntu-20.04
    docker pull sen4x/maja:3.2.2-centos-7
    docker pull sen4x/l2a-l8-alignment:0.1
    docker pull sen4x/l2a-dem:0.1

    mkdir -p /var/lib/t-rex
    chown ${SYS_ACC_NAME}: /var/lib/t-rex

    # the database should already be running since `migrate_postgres_to_docker`
    docker run --rm -u $(id -u $SYS_ACC_NAME):$(id -g $SYS_ACC_NAME) -v /etc/sen2agri/sen2agri.conf:/etc/sen2agri/sen2agri.conf -v /var/lib/t-rex:/var/lib/t-rex sen4cap/data-preparation:0.1 t-rex-genconfig.py /var/lib/t-rex/t-rex.toml

    cd docker
    docker-compose up -d

    cd ..
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
            #set qos number of jobs able to run at any given time
            #add already created qos to user , and another qos if that qos already exists
            sacctmgr -i add qos "${qosName}" set GrpJobs=1
            sacctmgr -i modify user "${SYS_ACC_NAME}" set qos+="${qosName}"
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

function install_snap() {
    # Install and config SNAP
    # check if docker image already exists
    # TODO: "docker image inspect sen4cap/snap" might be also used instead images -q
    if [[ "$(docker images -q sen4cap/snap:8.0 2> /dev/null)" == "" ]]; then
        TARGET_SNAP_TMP_DIR="/mnt/archive/temp/$(date +%Y%m%d%H%M%S)/"
        echo "Using directory ${TARGET_SNAP_TMP_DIR} for SNAP image build working dir ..."
        mkdir -p ${TARGET_SNAP_TMP_DIR} && \
        cp -fR ./docker/snap8 ${TARGET_SNAP_TMP_DIR} && \
        wget -P ${TARGET_SNAP_TMP_DIR}/snap8/ http://step.esa.int/downloads/8.0/installers/esa-snap_sentinel_unix_8_0.sh && \
        chmod +x ${TARGET_SNAP_TMP_DIR}/snap8/esa-snap_sentinel_unix_8_0.sh && \
        docker build -t sen4cap/snap:8.0 -f ${TARGET_SNAP_TMP_DIR}/snap8/Dockerfile ${TARGET_SNAP_TMP_DIR}/snap8/
        if [ -d ${TARGET_SNAP_TMP_DIR} ] ; then
            echo "Removing ${TARGET_SNAP_TMP_DIR} ..."
            rm -fR ${TARGET_SNAP_TMP_DIR}
        fi
    else
        echo "No need to install SNAP container, it already exists ..."
    fi
}

# TODO: This should be removed when implemented in the services or in processors
function copy_additional_scripts() {
    cp -fR ./s4c_l4c_export_all_practices.py /usr/bin
}

systemctl stop sen2agri-scheduler sen2agri-executor sen2agri-orchestrator sen2agri-http-listener sen2agri-demmaccs sen2agri-demmaccs.timer sen2agri-monitor-agent sen2agri-services

# TODO: This should be removed when implemented in the services or in processors
copy_additional_scripts

migrate_to_docker

yum -y install python-dateutil libcurl-devel openssl-devel libxml2-devel php-pgsql
yum -y install ../rpm_binaries/*.rpm
rm -rf /usr/local/bin/gdal_edit.py
yum -y remove sen2agri-website

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

# Update the QOS list if any new qos was added meanwhile
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

if [ "$DB_NAME" == "sen2agri" ] ; then
    # Reset the download failed products
    resetDownloadFailedProducts
else
    install_snap
fi

if [ ! -d /var/log/sen2agri ]; then
    mkdir -p /var/log/sen2agri
    chown ${SYS_ACC_NAME}: /var/log/sen2agri
fi

systemctl start sen2agri-executor sen2agri-orchestrator sen2agri-http-listener sen2agri-demmaccs sen2agri-demmaccs.timer sen2agri-monitor-agent sen2agri-scheduler sen2agri-services


