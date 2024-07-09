#!/bin/sh

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

MAJA_VER="4.5.4"
JAVA_VER=22

source ${SCRIPTPATH}/common_functions.sh

: ${SYS_ACC_NAME:="sen2agri-service"}


function install_sen2agri_services()
{
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

                if ! grep -q "gdal.auxdata.path" ${TARGET_SERVICES_DIR}/config/services.properties
                then
                    echo "Updating the gdal auxdata infos ..."
                    echo -e "\r\n\r\n################################################\r\n##GDAL Tile Cache config\r\n##\r\n##Cache activation\r\ngdal.tile.cache.enabled=true\r\n##Cache location\r\ngdal.tile.cache.dir=.eds/zxy-tiles-cache/\r\n##Size of cache (MB)\r\ngdal.tile.cache.size=1024\r\n##Clear cache at startup\r\ngdal.tile.cache.clear=false\r\ngdal.auxdata.path=/mnt/archive/snap_tmp\r\n\r\n################################################\r\n## Miscellaneous settings\r\nquicklook.extension=.png" >> ${TARGET_SERVICES_DIR}/config/services.properties

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
    
    # Update the start.sh for Arrow IPC on Java 17
    echo "Updating start.sh for Arrow IPC ..."
    sed -i 's/java -cp/java --add-opens=java.base\/java.nio=ALL-UNNAMED -cp/g' ${TARGET_SERVICES_DIR}/bin/start.sh
    
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
    echo "Resetting failed downloaded products from table downloader_history in database $CONFIGURATION_DB_NAME ..."
    psql -U postgres $CONFIGURATION_DB_NAME -c "update downloader_history set no_of_retries = '0' where status_id = '3' "
    psql -U postgres $CONFIGURATION_DB_NAME -c "update downloader_history set no_of_retries = '0' where status_id = '4' "
    psql -U postgres $CONFIGURATION_DB_NAME -c "update downloader_history set status_id = '3' where status_id = '4' "
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
        yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
        yum -y install docker-ce docker-ce-cli containerd.io docker-compose gdal jq
        systemctl enable docker
        systemctl restart docker
    fi
    usermod -aG docker ${SYS_ACC_NAME}
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
    yum -y autoremove otb sen2agri-processors gdal-libs gdal-python geos geos38 libgeotiff proj49

    echo "Installing Postgres client libraries and tools"
    yum -y install https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
    yum -y update pgdg-redhat-repo
    yum -y install postgresql16 python-psycopg2 gdal-python

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
    docker pull osgeo/gdal:ubuntu-full-3.4.1
    docker pull sen4x/fmask_extractor:0.1.2
    docker pull sen4x/fmask:4.4-ubuntu-20.04

    # TODO : Remove this when the image is published
    docker load < docker/images/sen4cap_data_preparation_2_0.tar.gz
    docker load < docker/images/sen4cap_processors_3.3.0.tar.gz
    docker pull sen4cap/processors:3.3.0

    docker load < docker/images/sen4cap_processors_scripts_3.3.0.tar.gz
    docker pull sen4cap/processors-scripts:3.3.0

    docker load < docker/images/sen4x_processors_new_0.1.0.tar.gz
    docker pull sen4x/processors-new:0.1.0

    docker load < docker/images/sen4x_era5_0.0.1.tar.gz
    docker pull sen4x/era5-weather:0.0.1
    docker load < docker/images/sen4stat_processors_1.0.0.tar.gz
    docker pull sen4stat/processors:1.0.0
    docker pull sen4cap/data-preparation:0.1
    docker pull sen4cap/data-preparation:0.2
    docker pull sen4cap/data-preparation:0.3
    docker pull sen4cap/grassland_mowing:3.0.0
    
    docker pull sen4x/l2a-processors:0.2.3
    docker pull sen4x/sen2cor:2.10.01-ubuntu-20.04
    docker pull sen4x/maja:${MAJA_VER}-centos-7
    docker pull sen4x/l2a-l8-alignment:0.1.2
    docker pull sen4x/l2a-dem:0.1.3

    mkdir -p /var/lib/t-rex
    chown ${SYS_ACC_NAME}: /var/lib/t-rex

    # the database should already be running since `migrate_postgres_to_docker`
    docker run --rm -u $(id -u $SYS_ACC_NAME):$(id -g $SYS_ACC_NAME) -v /etc/sen2agri/sen2agri.conf:/etc/sen2agri/sen2agri.conf -v /var/lib/t-rex:/var/lib/t-rex sen4cap/data-preparation:0.1 t-rex-genconfig.py /var/lib/t-rex/t-rex.toml

    cd docker
    
    # we don't want to do this when upgrading ...
    # docker-compose up -d

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
    VAL=$(psql -qtAX -U admin ${CONFIGURATION_DB_NAME} -c "select value from config where key = 'processor.l2a.maja.gipp-path' and site_id is null")
    if [ ! -z $VAL ] ; then
        src_maja_dir=""
        if [ -d ../gipp_maja ] ; then
            src_maja_dir=../gipp_maja/
            echo "Using as MAJA gipp source the directory ${src_maja_dir} ..."
            
        elif [ -d ../gipp_maja_${MAJA_VER} ] ; then 
            src_maja_dir=../gipp_maja_${MAJA_VER}/
            echo "Using as MAJA gipp source the directory ${src_maja_dir} ..."

        elif [ -f ../gipp_maja_${MAJA_VER}.zip ] ; then
            mkdir ../gipp_maja
            src_maja_dir=../gipp_maja/
            unzip ../gipp_maja_${MAJA_VER}.zip -d ${src_maja_dir}
            echo "Using as MAJA gipp source the content of file ../gipp_maja_${MAJA_VER}.zip extacted to directory ${src_maja_dir} ..."
        else 
            echo "WARNING: Key processor.l2a.maja.gipp-path found in config table for database ${CONFIGURATION_DB_NAME} with value $VAL but the directory does not exists for this value. UserConfiguration not updated ..."
        fi
        if [ ! -z ${src_maja_dir} ] && [ -d ${src_maja_dir} ] ; then
            # Performing backup only if we have source MAJA gips 
            if [ -d $VAL ] ; then
                echo "Key processor.l2a.maja.gipp-path found with value ${VAL}. Performing backup ..."
                dt=$(date '+%Y%m%d_%H%M%S');
                mv ${VAL} "${VAL}"_backup_"${dt}"
            fi

            mkdir -p "${VAL}"
            echo "Copying GIPP from directory ${src_maja_dir} to destination $VAL ..."
            cp -fR ${src_maja_dir}/* "${VAL}"
            
            cp -fR ./config/maja/UserConfiguration ${VAL}
        fi
    else
        echo "WARNING: Key processor.l2a.maja.gipp-path not found in config table for database ${CONFIGURATION_DB_NAME}. UserConfiguration not updated ..."
    fi
}

function vercomp () {
    if [[ $1 == $2 ]]
    then
        return 0
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++))
    do
        ver1[i]=0
    done
    for ((i=0; i<${#ver1[@]}; i++))
    do
        if [[ -z ${ver2[i]} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]}))
        then
            return 1
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]}))
        then
            return 2
        fi
    done
    return 0
}

function install_java()
{
    install_java="0"
    if type -p java; then
        echo found java executable in PATH
        _java=java
    elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
        echo "Found java executable in $JAVA_HOME"
        _java="$JAVA_HOME/bin/java"
    else
        echo "No java found."
        install_java="1"
    fi
    
    if [[ "$_java" ]]; then
        version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
        echo "Java version is "$version" "
        vercomp "${version}" "${JAVA_VER}"
        if [[ "$?" == "0" || "$?" == "1" ]]; then
            echo "Java version is more than ${JAVA_VER}. Nothing to do"
        else
            echo "Version is less than ${JAVA_VER}."
            install_java="1"
        fi
    fi
    if [[ "$install_java" == "1" ]] ; then
        echo "Installing java ${JAVA_VER}"
        # installing java
        wget https://download.oracle.com/java/${JAVA_VER}/latest/jdk-${JAVA_VER}_linux-x64_bin.rpm -P /tmp/
        rpm -ivh /tmp/jdk-${JAVA_VER}_linux-x64_bin.rpm
        rm -f /tmp/jdk-${JAVA_VER}_linux-x64_bin.rpm
    fi
}

# TODO: This should be removed when implemented in the services or in processors
function copy_additional_scripts() {
    cp -fR ./s4c_l4c_export_all_practices.py /usr/bin
}

# Load profile installation configuration
load_configuration

systemctl stop sen2agri-scheduler sen2agri-executor sen2agri-orchestrator sen2agri-http-listener sen2agri-demmaccs sen2agri-demmaccs.timer sen2agri-monitor-agent sen2agri-services sen2agri-fmask.timer sen2agri-fmask sen2agri-era5-downloader.timer sen2agri-era5-downloader

# TODO: This should be removed when implemented in the services or in processors
copy_additional_scripts

migrate_to_docker

yum -y install python-dateutil libcurl-devel openssl-devel libxml2-devel php-pgsql
yum -y install ../rpm_binaries/*.rpm

TARGET_SERVICES_DIR="/usr/share/sen2agri/sen2agri-services"

install_java
install_sen2agri_services

# Update the QOS list if any new qos was added meanwhile
create_and_config_slurm_qos

ldconfig

# run specific configuration migration scripts
for profile in ${CONFIGURATION_PROFILES[@]} ; do
    run_migration_scripts "migrations/${profile}" "${CONFIGURATION_DB_NAME}"
done

# run common scripts
# run_migration_scripts "migrations/" "${CONFIGURATION_DB_NAME}"

update_maja_gipp

systemctl daemon-reload

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

# In some previus versions, these were not enabled so make sure they are enabled
systemctl enable sen2agri-fmask
systemctl enable sen2agri-fmask.timer
systemctl enable sen2agri-era5-downloader
systemctl enable sen2agri-era5-downloader.timer 

systemctl start sen2agri-executor sen2agri-orchestrator sen2agri-http-listener sen2agri-demmaccs sen2agri-demmaccs.timer sen2agri-monitor-agent sen2agri-scheduler sen2agri-services sen2agri-fmask.timer sen2agri-fmask sen2agri-era5-downloader.timer sen2agri-era5-downloader


