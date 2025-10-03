#!/bin/bash
#set -x #echo on

##
## SCRIPT: INSTALL AND CONFIGURE PLATFORM SEN2AGRI
##
##
## SCRIPT STEPS
##     - INSTALL OTB, SEN2AGRI PROCESSORS AND SEN2AGRI SERVICE
##     - INSTALL SLURM
##     - CONFIGURE SLURM
##         - PARSE AND UPDATE SLURM.CONF FILE AND SLURMDB.CONF FILE
##         - COPY SLURM.CONF AND SLURMDB.CONF INTO FOLDER /ETC/SLURM/
##         - INSTALL MUNGE SERVICE FOR SLURM AND START IT
##         - INSTALL MYSQL(MARIADB), CREATE SLURM DATABASE
##         - START SLURM DAEMONS: SLURMDB, SLURMCTLD, SLURMD AND SLURM
##         - CREATE SLURM ACCOUNT AND USER
##         - CREATE AND CONFIGURE SLURM QOS
##     - INSTALL, CONFIGURE POSTGRESQL AND CREATE DATABASE FOR SEN2AGRI PLATFORM
##     - INSTALL, CONFIGURE APACHE, PHP AND SEN2AGRI WEBSITE ON THE PLATFORM
##     - INSTALL SEN2AGRI DOWNLOADERS AND START SERVICE ON THE PLATFORM
##     - INSTALL SEN2AGRI DEMMACS AND START SERVICE ON THE PLATFORM
################################################################################################
## SCRIPT USAGE:
##
## open a terminal go into delivery install_script folder:
## cd /path/to/install_script
## sudo ./install.sh
################################################################################################

source ./common_functions.sh

#-----------------------------------------------------------------------------------------#
: ${SYS_ACC_NAME:="sen2agri-service"}
: ${SLURM_ACC_NAME:="slurm"}
: ${MUNGE_ACC_NAME:="munge"}
#-----------------------------------------------------------------------------------------#
: ${SLURM_CONF_PATH:="/etc/slurm"}
: ${SLURM_CLUSTER_NAME:="sen2agri"}
: ${SLURM_MACHINE_NOCPUS:=$(cat /proc/cpuinfo | grep processor | wc -l)}
: ${SRC_SLURM_CONFIG:="slurm_rocky.conf"}
: ${SLURM_CONFIG:="slurm.conf"}
: ${SLURM_CONFIG_DB:="slurmdbd.conf"}

# Default services identifiers 
SERVICES_IDENTIFIER="sen2agri-services"
EXECUTOR_SERVICE_IDENTIFIER="sen2agri-executor"
EXECUTOR_TIMER_IDENTIFIER="sen2agri-executor.timer"
ORCHESTRATOR_SERVICE_IDENTIFIER="sen2agri-orchestrator"
SCHEDULER_SERVICE_IDENTIFIER="sen2agri-scheduler"
HTTP_LISTENER_SERVICE_IDENTIFIER="sen2agri-http-listener"
MONITOR_AGENT_SERVICE_IDENTIFIER="sen2agri-monitor-agent"

JAVA_VER=22

#----------------SLURM MYSQL DATABASE CREATION---------------------------------------------#
MYSQL_DB_CREATION="create database slurm_acct_db;create user slurm;
set password for slurm = password('sen2agri');"
MYSQL_DB_ACCESS_GRANT="grant usage on *.* to slurm;grant all privileges on slurm_acct_db.* to slurm;flush privileges;"
MYSQL_CMD=${MYSQL_DB_CREATION}${MYSQL_DB_ACCESS_GRANT}
#------------------------------------------------------------------------------------------#
declare -r -i -g L1C_PROCESSOR_SEN2COR=1
declare -r -i -g L1C_PROCESSOR_MAJA=2
#------------------------------------------------------------------------------------------#

function join_by { local IFS="$1"; shift; echo "$*"; }

#-----------------------------------------------------------#

function parse_and_update_slurm_conf_file()
{
   ####################################
   ####  copy conf files to /etc/slurm
   ####################################
   mkdir -p ${SLURM_CONF_PATH}
   cp -f $(find ./ -name ${SRC_SLURM_CONFIG}) ${SLURM_CONF_PATH}/${SLURM_CONFIG}
   cp -f $(find ./ -name ${SLURM_CONFIG_DB}) ${SLURM_CONF_PATH}
   chown slurm:slurm ${SLURM_CONF_PATH}/${SLURM_CONFIG_DB}
   chmod 600 ${SLURM_CONF_PATH}/${SLURM_CONFIG_DB}

   sed -ri "s|CPUs=.+|CPUs=${SLURM_MACHINE_NOCPUS}|g" /etc/slurm/slurm.conf
}
#-----------------------------------------------------------#
function create_slurm_data_base()
{
   ##install expect
   # yum -y install expect expectk
   dnf -y install expect

   ##install mysql (mariadb)
   yum -y install mariadb-server mariadb

   ##start mysql (mariadb)
   systemctl start mariadb

   ##enable mysql (mariadb) to start at boot
   systemctl enable mariadb

   ##get status of mysql service
   echo "MYSQL SERVICE: $(systemctl status mariadb | grep "Active")"

   ##install secure mysql
   SECURE_MYSQL=$(expect -c "
      set timeout 10
      spawn mysql_secure_installation

      expect \"Enter current password for root (enter for none):\"
      send \"\r\"

      expect \"Change the root password?\"
      send \"n\r\"

      expect \"Remove anonymous users?\"
      send \"y\r\"

      expect \"Disallow root login remotely?\"
      send \"y\r\"

      expect \"Remove test database and access to it?\"
      send \"y\r\"

      expect \"Reload privilege tables now?\"
      send \"y\r\"
      expect eof
   ")

   echo "$SECURE_MYSQL"

   ##create database for slurm service
   DB_MYSQL=$(expect -c "
      set timeout 5
      spawn mysql -u root -p -e \"${MYSQL_CMD}\"
      expect \"Enter password:\"
      send \"\r\"
      expect eof
   ")

   echo "$DB_MYSQL"
   echo "Sleeping 5 seconds waitinig for MariaDB..."
   sleep 5
}
#-----------------------------------------------------------#
function config_and_start_slurm_service()
{
   #create slurm account for service install
   adduser -m ${SLURM_ACC_NAME}

   ####################################
   ####  process SLURM .conf files
   ####################################
   parse_and_update_slurm_conf_file

   ####################################
   ####  SLURM database create and config
   ####################################
   create_slurm_data_base

   ####################################
   ####  SLURM Daemons start
   ####################################
   ##start slurmdbd (slurmdbd)
   systemctl start slurmdbd

   ##enable slurmdbd (slurmdbd)  to start at boot
   systemctl enable slurmdbd

   echo "Sleeping 5 seconds waitinig for slurmdbd..."
   sleep 5

   ##get status of slurmdbd service
   echo "SLURM DB SERVICE: $(systemctl status slurmdbd | grep "Active")"

   ##create the cluster in the accounting system
   sacctmgr -i add cluster "${SLURM_CLUSTER_NAME}"

   ##create SLURM spool and log directories and set permissions accordingly
   mkdir /var/spool/slurm
   chown -R slurm:slurm /var/spool/slurm
   mkdir /var/log/slurm
   chown -R slurm:slurm /var/log/slurm

   ##start slurm controller daemon slurmctld (slurmctld)
   systemctl start slurmctld

   ##enable slurm controller daemon slurmctld to start at boot
   systemctl enable slurmctld

   ##get status of slurm controller daemon  slurmctld service
   echo "SLURM CTL SERVICE: $(systemctl status slurmctld | grep "Active")"

   ##start slurm node daemon slurmd (slurmd)
   systemctl start slurmd

   ##enable slurm node daemon slurmd to start at boot
   systemctl enable slurmd

   ##get status of slurm node daemon slurmd service
   echo "SLURM NODE SERVICE: $(systemctl status slurmd | grep "Active")"

   ##start slurm service (slurm)
   # systemctl start slurm

   ##enable slurm service to start at boot
   # systemctl enable slurm

   ##get status of slurm service service
   echo "SLURM SERVICE: $(systemctl status slurm | grep "Active")"

   ####################################
   ####  SLURM post config
   ####################################
   ## create account in slurm
   create_slurm_account

   ## create QOS in slurm
   create_and_config_slurm_qos
}
#-----------------------------------------------------------#

function config_and_start_munge_service()
{
   #create munge account for service install
   adduser -m ${MUNGE_ACC_NAME}

   ##secure installation  - set permissions on munge folders
   chmod 755 /etc/munge
   chmod 755 /var/lib/munge/
   chmod 755 /var/log/munge/
   chmod 755 /var/run/munge/

   ##generate MUNGE key
   dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key
   chown munge:munge /etc/munge/munge.key
   chmod 400 /etc/munge/munge.key

   ##enable munge Daemon to start on boot time
   systemctl enable munge

   ##start munge Daemon
   systemctl start munge

   ##get status of munge daemon
   echo "MUNGE SERVICE: $(systemctl status munge | grep "Active")"

}
#-----------------------------------------------------------#
function create_system_account()
{
   #create system account for running services
   adduser -m ${SYS_ACC_NAME}
   usermod -aG docker ${SYS_ACC_NAME}
}
#-----------------------------------------------------------#
function create_slurm_account()
{
   #create SLURM account for running application
   sacctmgr -i add account "${SYS_ACC_NAME}"

   #create user associated to the account
   sacctmgr -i add user "${SYS_ACC_NAME}" Account="${SYS_ACC_NAME}" AdminLevel=Admin
}

#-----------------------------------------------------------#
function create_and_config_slurm_qos()
{
   #extract each configured QOS from SLURM_QOS_LIST
   IFS=',' read -ra ADDR <<< "${SLURM_QOS_LIST}"

   #for each qos defined in configuration
   for qosName in "${ADDR[@]}"; do
        sacctmgr -i add qos "${qosName}" set GrpJobs=1
        sacctmgr -i modify user "${SYS_ACC_NAME}" set qos+="${qosName}"
   done
   sacctmgr -i modify user "${SYS_ACC_NAME}" set qos+=normal

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
#-----------------------------------------------------------#
function config_docker()
{
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
    docker run --rm -u $(id -u $SYS_ACC_NAME):$(id -g $SYS_ACC_NAME) -v /etc/sen2agri/${SERVICES_CONFIGURATION_NAME}.conf:/etc/sen2agri/sen2agri.conf -v /var/lib/t-rex:/var/lib/t-rex sen4cap/data-preparation:0.2 t-rex-genconfig.py --stub /var/lib/t-rex/t-rex.toml

    cd docker
    docker compose up -d

    RETRIES=120
    until docker compose exec db pg_isready || [ $RETRIES -eq 0 ]; do
        echo "Waiting for postgres, $RETRIES remaining attempts..."
        RETRIES=$((RETRIES-1))
        sleep 1
    done

    echo "Waiting 120 seconds for postgres to settle..."
    sleep 120

    RETRIES=120
    until docker compose exec db pg_isready || [ $RETRIES -eq 0 ]; do
        echo "Waiting for postgres, $RETRIES remaining attempts..."
        RETRIES=$((RETRIES-1))
        sleep 1
    done
    
    cd ..
}
#-----------------------------------------------------------#
function install_and_config_postgresql()
{
   # NB: the container uses `trust`, not `peer` for local connections

   # Install `psql` and client libraries
   yum -y install postgresql16

    #------------DATABASE CREATION------------#
    # Update database name
    sed -i -e "s|CREATE DATABASE .*|CREATE DATABASE ${CONFIGURATION_DB_NAME}|g" ./database/00-database/db.sql
    sed -i -e "s|-- DataBase Create:.*|-- DataBase Create: ${CONFIGURATION_DB_NAME}|g" ./database/00-database/db.sql

    sed -i -e "s|GRANT ALL PRIVILEGES ON DATABASE .* TO|GRANT ALL PRIVILEGES ON DATABASE ${CONFIGURATION_DB_NAME} TO|g" ./database/09-privileges/privileges.sql
    sed -i -e "s|-- Privileges:.*|-- Privileges: ${CONFIGURATION_DB_NAME}|g" ./database/09-privileges/privileges.sql

    # Insert the user name and password
    sed -i -e "s|select sp_adduser('.*',|select sp_adduser('$USER_NAME', '$USER_MAIL', '$USER_PASS',|g" ./database/07-data/13.user.sql

    # first, the database is created. the privileges will be set after all
    # the tables, data and other stuff is created (see down, privileges.sql
    cat "$(find ./ -name "database")/00-database"/db.sql | psql -U postgres

    #run scripts populating database
    populate_from_scripts "$(find ./ -name "database")/01-extensions"
    populate_from_scripts "$(find ./ -name "database")/02-types"
    populate_from_scripts "$(find ./ -name "database")/03-tables"
    populate_from_scripts "$(find ./ -name "database")/04-views"
    populate_from_scripts "$(find ./ -name "database")/05-functions"
    populate_from_scripts "$(find ./ -name "database")/06-indexes"
    populate_from_scripts "$(find ./ -name "database")/07-data"
    populate_from_scripts "$(find ./ -name "database")/08-keys"
    # granting privileges to sen2agri-service and admin users
    populate_from_scripts "$(find ./ -name "database")/09-privileges"
    populate_from_scripts "$(find ./ -name "database")/10-triggers"
    populate_from_scripts "$(find ./ -name "database")/11-customizations"
    
    filter_required_components
   
}
#-----------------------------------------------------------#
function populate_from_scripts()
{
    local curPath=$1
    # check first if it contains any sql files 
    count=`ls -1 ${curPath}/*.sql 2>/dev/null | wc -l`
    #for each sql scripts found in this folder
    if [ ${count} != 0 ]; then 
        for scriptName in "${curPath}"/*.sql ; do
            scriptToExecute=${scriptName}
            ## perform execution of each sql script
            echo "Executing SQL script: $scriptToExecute"
            cat "$scriptToExecute" | psql -U postgres "${CONFIGURATION_DB_NAME}"
            # execute also the script in the profiles directories. We need to execute it now as other things may depend on this update
            # The scripts are executed in the order how the profiles were defined
            for profile in ${CONFIGURATION_PROFILES[@]} ; do
                local customProfileScriptsPath=${curPath}/${profile}
                customScriptToExecute=""
                if [ -d "$customProfileScriptsPath" ]; then
                    scriptFileName=$(basename -- "$scriptName")

                    if [ -f ${customProfileScriptsPath}/${scriptFileName} ]; then
                        customScriptToExecute=${customProfileScriptsPath}/${scriptFileName}
                    fi
                fi
                if ! [[ -z $customScriptToExecute ]] ; then
                    echo "Executing SQL script: $customScriptToExecute"
                    cat "$customScriptToExecute" | psql -U postgres "${CONFIGURATION_DB_NAME}"
                fi
            done
        done
    fi
    # Now check for the configuration profiles folders if there are new scripts, others than the default ones. In this case, we must execute them too
    for profile in ${CONFIGURATION_PROFILES[@]} ; do
        local customProfileScriptsPath=${curPath}/${profile}
        if [ -d "$customProfileScriptsPath" ]; then
            count=`ls -1 ${customProfileScriptsPath}/*.sql 2>/dev/null | wc -l`
            if [ ${count} != 0 ]; then
                for scriptName in "$customProfileScriptsPath"/*.sql
                do
                    scriptToExecute=${scriptName}
                    scriptFileName=$(basename -- "$scriptName")
                    if [[ ! -f ${curPath}/${scriptFileName} ]]; then
                        ## perform execution of each sql script
                        echo "Executing SQL script: $scriptToExecute"
                        cat "$scriptToExecute" | psql -U postgres "${CONFIGURATION_DB_NAME}"
                    fi
                done
            fi
        fi
    done
}
#-----------------------------------------------------------#
function install_downloaders_demmacs()
{
   mkdir /var/log/sen2agri
   chown ${SYS_ACC_NAME}: /var/log/sen2agri

   ##install prerequisites for Downloaders
   yum -y install wget python-lxml bzip2 python-beautifulsoup4 python-dateutil java-1.8.0-openjdk

   ##install Sen2Agri Downloaders  & Demmacs
   if [ -f ../rpm_binaries/${SERVICES_CONFIGURATION_NAME}-downloaders-demmaccs-*.centos7.x86_64.rpm ] ; then 
      yum -y install ../rpm_binaries/${SERVICES_CONFIGURATION_NAME}-downloaders-demmaccs-*.centos7.x86_64.rpm
   else 
      yum -y install ../rpm_binaries/sen2agri-downloaders-demmaccs-*.centos7.x86_64.rpm
   fi
   
   #reload daemon to update it with new services
   systemctl daemon-reload

   # `systemctl enable --now` doesn't work for timers on CentOS 7
   systemctl enable sen2agri-demmaccs.timer
   systemctl start sen2agri-demmaccs.timer

}
#-----------------------------------------------------------#
function install_RPMs()
{
   ##########################################################
   ####  OTB, SEN2AGRI-PROCESSORS, SEN2AGRI-SERVICES
   ##########################################################

   ##install a couple of packages
   yum -y install python3-gdal python3-psycopg2 python-dateutil gd

   ##install Orfeo ToolBox
   yum -y install ../rpm_binaries/otb-*.rpm

   ##install Sen2Agri Processors
   yum -y install ../rpm_binaries/sen2agri-processors-*.centos7.x86_64.rpm

   ##install Sen2Agri Services
   if [ -f ../rpm_binaries/${SERVICES_CONFIGURATION_NAME}-app-*.centos7.x86_64.rpm ] ; then 
      yum -y install ../rpm_binaries/${SERVICES_CONFIGURATION_NAME}-app-*.centos7.x86_64.rpm
   else 
      yum -y install ../rpm_binaries/sen2agri-app-*.centos7.x86_64.rpm
   fi

   ##install SLURM
   yum -y install slurm slurm-slurmctld slurm-slurmd slurm-devel slurm-pam_slurm slurm-perlapi slurm-slurmdbd slurm-torque slurm-libs
}

#-----------------------------------------------------------#
function install_additional_packages()
{
    if [ "${USE_SNAP}" == "1" ] ; then
        install_snap
    fi
}


function maccs_or_maja()
{
#    while [[ $answer != '1' ]] && [[ $answer != '2' ]]
#    do
#	read -n1 -p "What L1C processor should be used? (1 for Sen2Cor / 2 for MAJA): " -r answer
#	printf "\n"
#	case $answer in
#	    1)
#		echo "Sen2Cor will be used as L1C processor"
#		l1c_processor=$L1C_PROCESSOR_SEN2COR
# 		;;
#	    2)
#		echo "MAJA will be used as L1C processor"
#		l1c_processor=$L1C_PROCESSOR_MAJA
#		;;
#	    *)
#		echo "Unknown answer"
#		;;
#	esac
#    done
    l1c_processor=$L1C_PROCESSOR_MAJA
    case $l1c_processor in
    $L1C_PROCESSOR_SEN2COR)
	l1c_processor_name="Sen2Cor"
	l1c_processor_bin="sen2cor"
	# l1c_processor_path="/opt/maccs/core"
	l1c_processor_gipp_destination="/mnt/archive/gipp/sen2cor"
	l1c_processor_gipp_source="../gipp_sen2cor"
	;;
    $L1C_PROCESSOR_MAJA)
	l1c_processor_name="MAJA"
	l1c_processor_bin="maja"
	# l1c_processor_path="/opt/maja"
	l1c_processor_gipp_destination="/mnt/archive/gipp/maja"
	l1c_processor_gipp_source="../gipp_maja"
	;;
    *)
	echo "Unknown L1C processor...exit "
	exit
	;;
    esac
}

function check_paths()
{
    echo "Checking paths..."

    if [ ! -d /mnt/archive ]; then
        echo "Please create /mnt/archive with mode 777."
        echo "Actually only the sen2agri-service and apache users require access to the directory, but the installer does not support that."
        echo "Exiting now"
        exit 1
    fi

    out=($(stat -c "%a %U" /mnt/archive))
    if [ "${out[0]}" != "777" ] && [ "${out[1]}" != "sen2agri-service" ]; then
        read -p "/mnt/archive should be writable by sen2agri-service. Continue? (y/n) "
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Exiting now"
            exit 1
        fi
    fi

    if ! ls -A /mnt/archive/srtm > /dev/null 2>&1; then
        if [ -f ../srtm.zip ]; then
            mkdir -p /mnt/archive/srtm && unzip ../srtm.zip -d /mnt/archive/srtm
            if [ $? -ne 0 ]; then
                echo "Unable to unpack the SRTM dataset into /mnt/archive/srtm"
                echo "Exiting now"
                exit 1
            fi
        else
            echo "Please unpack the SRTM dataset into /mnt/archive/srtm"
            echo "Exiting now"
            exit 1
        fi
    fi

    if ! ls -A /mnt/archive/swbd > /dev/null 2>&1; then
        if [ -f ../swbd.zip ]; then
            mkdir -p /mnt/archive/swbd && unzip ../swbd.zip -d /mnt/archive/swbd
            if [ $? -ne 0 ]; then
                echo "Unable to unpack the SWBD dataset into /mnt/archive/swbd"
                echo "Exiting now"
                exit 1
            fi
        else
            echo "Please unpack the SWBD dataset into /mnt/archive/swbd"
            echo "Exiting now"
            exit 1
        fi
    fi

    if [ ! -d /mnt/upload ]; then
        echo "Please create /mnt/upload making sure it's writable by the apache user and readable by sen2agri-service."
        echo "Exiting now"
        exit 1
    fi

    out=($(stat -c "%a %U" /mnt/upload))
    if [ "${out[0]}" != "777" ] && [ "${out[1]}" != "apache" ]; then
        read -p "/mnt/upload should be writable by apache. Continue? (y/n) "
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Exiting now"
            exit 1
        fi
    fi

    if ! ls -A $l1c_processor_gipp_destination > /dev/null 2>&1; then
        l1c_processor_gipp_source_tmp="../gipp_maja"
        if [ -d $l1c_processor_gipp_source ]; then
            l1c_processor_gipp_source_tmp=$l1c_processor_gipp_source
        elif [ -d "$l1c_processor_gipp_source"_"${MAJA_VER}" ]; then
            l1c_processor_gipp_source_tmp="$l1c_processor_gipp_source"_"${MAJA_VER}"
        fi
        if [ -d $l1c_processor_gipp_source_tmp ]; then
            echo "Copying $l1c_processor_name GIPP files to ${l1c_processor_gipp_destination}"
            mkdir -p ${l1c_processor_gipp_destination}
            cp -rf "${l1c_processor_gipp_source_tmp}"/* ${l1c_processor_gipp_destination}

            echo "Copying UserConfiguration into maja gipp location ..."
            cp -fR ./config/maja/UserConfiguration ${l1c_processor_gipp_destination}
            
        else
            echo "Cannot find $l1c_processor_name GIPP files in the distribution, please copy them to $l1c_processor_gipp_destination"
        fi
    fi

    if [ "$INSTALL_CCI_LC" == "1" ] ; then 
        echo "Creating /mnt/archive/reference_data"
        mkdir -p /mnt/archive/reference_data
        echo "Copying reference data"
        if [ -d ../reference_data/ ]; then
            cp -rf ../reference_data/* /mnt/archive/reference_data
        fi
    fi
}

# Update /etc/sen2agri/sen2agri.conf with the right database
function updateInstalledConfigurationParams()
{
    echo "Updating /etc/sen2agri/${SERVICES_CONFIGURATION_NAME}.conf ... "
    sed -i -e "s|DatabaseName=sen2agri|DatabaseName=$CONFIGURATION_DB_NAME|g" /etc/sen2agri/${SERVICES_CONFIGURATION_NAME}.conf
    echo "Updating /etc/sen2agri/${SERVICES_CONFIGURATION_NAME}-monitor-agent.conf ... "
    sed -i -e "s|ServiceUrl=8082|${HTTP_LISTENER_PORT}|g" /etc/sen2agri/${SERVICES_CONFIGURATION_NAME}-monitor-agent.conf

    if [ "${SERVICES_CONFIGURATION_NAME}" != "sen2agri" ]; then
        # Updating the systemd services
        find /usr/lib/systemd/system/ -name "${SERVICES_CONFIGURATION_NAME}*" -exec basename {} ';' | while read name ; do 
            echo "Updating  /usr/lib/systemd/system/${name} ... "
            sed -i -e "s|for Sen2Agri|for ${SERVICES_CONFIGURATION_NAME}|g" /usr/lib/systemd/system/${name}
            sed -i -e "s|/sen2agri-|/${SERVICES_CONFIGURATION_NAME}-|g" /usr/lib/systemd/system/${name}
            
        done
        # sed -i -e "s|Services for Sen2Agri|Services for ${SERVICES_CONFIGURATION_NAME}|g" /usr/lib/systemd/system/${SERVICES_CONFIGURATION_NAME}-services.service
        # sed -i -e "s|/usr/share/sen2agri/sen2agri-services/bin/start.sh|/usr/share/sen2agri/${SERVICES_CONFIGURATION_NAME}-services/bin/start.sh|g" /usr/lib/systemd/system/${SERVICES_CONFIGURATION_NAME}-services.service

        psql -U admin ${CONFIGURATION_DB_NAME} -c "update config set value = '${ORCHESTRATOR_HTTP_LISTEN_PORT}' where key = 'orchestrator.http-server.listen-port'"
        psql -U admin ${CONFIGURATION_DB_NAME} -c "update config set value = '${EXECUTOR_HTTP_LISTEN_PORT}' where key = 'executor.http-server.listen-port'"
        psql -U admin ${CONFIGURATION_DB_NAME} -c "update config set value = '${EXECUTOR_LISTEN_PORT}' where key = 'executor.listen-port'"
        psql -U admin ${CONFIGURATION_DB_NAME} -c "update config set value = '${HTTP_LISTENER_PORT}' where key = 'http-listener.listen-port'"
    fi
}

function update_website()
{
    echo "TODO: Update site accordingly ..."    
    echo "Updating /etc/sen2agri/${SERVICES_CONFIGURATION_NAME}.conf ... "
    echo "TODO: Update icons and other staff in website ..."    

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

    if [ -z ${zipArchive} ] ; then
        echo "No services zip archive provided in ../sen2agri-services or no ../sen2agri-services/${SERVICES_ARCHIVE} ..."
        echo "Exiting now"
        exit 1
    else
        filename="${zipArchive%.*}"

        echo "Extracting into /usr/share/sen2agri/${SERVICES_IDENTIFIER} from archive $zipArchive ..."

        mkdir -p /usr/share/sen2agri/${SERVICES_IDENTIFIER} && unzip ${zipArchive} -d /usr/share/sen2agri/${SERVICES_IDENTIFIER}
        if [ $? -ne 0 ]; then
            echo "Unable to unpack the sen2agri-services into/usr/share/sen2agri/${SERVICES_IDENTIFIER}"
            echo "Exiting now"
            exit 1
        fi
        # convert any possible CRLF into LF
        tr -d '\r' < /usr/share/sen2agri/${SERVICES_IDENTIFIER}/bin/start.sh > /usr/share/sen2agri/${SERVICES_IDENTIFIER}/bin/start.sh.tmp && cp -f /usr/share/sen2agri/${SERVICES_IDENTIFIER}/bin/start.sh.tmp /usr/share/sen2agri/${SERVICES_IDENTIFIER}/bin/start.sh && rm /usr/share/sen2agri/${SERVICES_IDENTIFIER}/bin/start.sh.tmp
        # ensure the execution flag
        chmod a+x /usr/share/sen2agri/${SERVICES_IDENTIFIER}/bin/start.sh
     
        # it might happen that some files to be packaged with the wrong read rights
        chmod -R a+r /usr/share/sen2agri/${SERVICES_IDENTIFIER}/

        # update the database name if needed in the sen2agri-services
        SERVICE_PROPERTIES_FILE="/usr/share/sen2agri/${SERVICES_IDENTIFIER}/config/services.properties"
        sed -i -e "s/sen4cap?stringtype=unspecified/${CONFIGURATION_DB_NAME}?stringtype=unspecified/" ${SERVICE_PROPERTIES_FILE}
        if ! grep -qF "gdal.auxdata.path" ${SERVICE_PROPERTIES_FILE}; then
            echo "gdal.auxdata.path=/mnt/archive/snap_tmp" >> ${SERVICE_PROPERTIES_FILE}
        fi
        
        # Update the site title 
        echo "Updating site title ..."
        sed -i -e "s|site.title = .*|site.title = $PROJECT_NAME|g" ${SERVICE_PROPERTIES_FILE}
        if [ -d "/usr/share/sen2agri/${SERVICES_IDENTIFIER}/static/assets/dist/img_${CONFIGURATION_NAME}" ] ; then
            echo "Copying logo images ..."
            cp -fR /usr/share/sen2agri/${SERVICES_IDENTIFIER}/static/assets/dist/img_${CONFIGURATION_NAME}/*.png /usr/share/sen2agri/${SERVICES_IDENTIFIER}/static/assets/dist/img
        else 
            echo "No special logo images found in /usr/share/sen2agri/${SERVICES_IDENTIFIER}/static/assets/dist/img_${CONFIGURATION_NAME}. Using the default ones from Sen4CAP ..."
        fi
        
    fi
    chown -R "${SYS_ACC_NAME}": /usr/share/sen2agri/${SERVICES_IDENTIFIER}/static/
}

function disable_selinux()
{
    echo "Disabling SELinux"
    echo "The Sen2Agri system is not inherently incompatible with SELinux, but relabelling the file system paths is not implemented yet in the installer."
    setenforce 0
    sed -i -e 's/SELINUX=enforcing/SELINUX=permissive/' /etc/selinux/config
}

# This is needed for SLURM because it uses dynamically-allocated ports
# The other services could do with a couple of rules
function disable_firewall()
{
    echo "Disabling the firewall"
    firewall-cmd --set-default-zone=trusted
    firewall-cmd --reload
}

# TODO: This should be removed when implemented in the services or in processors
function copy_additional_scripts() {
    cp -fR ./s4c_l4c_export_all_practices.py /usr/bin
}

###########################################################
##### MAIN                                              ###
###########################################################

if [ $EUID -ne 0 ]; then
    echo "This setup script must be run as root. Exiting now."
    exit 1
fi

# TODO: This should be removed when implemented in the services or in processors
copy_additional_scripts

# Load profile installation configuration
load_configuration

#use MACCS or MAJA?
maccs_or_maja

check_paths

disable_selinux
disable_firewall

##install EPEL for dependencies, PGDG for the Postgres client libraries and
yum -y install epel-release https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm yum-utils
dnf config-manager --disable pgdg13 pgdg14 pgdg15
yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
yum -y update epel-release pgdg-redhat-repo
dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin unzip gdal jq wget 

systemctl enable docker
systemctl restart docker

#-----------------------------------------------------------#
####  OTB, SEN2AGRI, SLURM INSTALL  & CONFIG     ######
#-----------------------------------------------------------#
## install binaries
install_RPMs

## create system account
create_system_account

## config and start munge
config_and_start_munge_service

## config and start slurm
config_and_start_slurm_service

config_docker

#-----------------------------------------------------------#
####  JAVA SERVICES INSTALLATION #####
#-----------------------------------------------------------#
install_java
install_sen2agri_services

#-----------------------------------------------------------#
####  POSTGRESQL INSTALL & CONFIG AND DATABASE CREATION #####
#-----------------------------------------------------------#
install_and_config_postgresql

#-----------------------------------------------------------#
####  DOWNLOADERS AND DEMMACS  INSTALL                  #####
#-----------------------------------------------------------#
install_downloaders_demmacs

#-----------------------------------------------------------#
####  ADDITIONAL PACKAGES      INSTALL                  #####
#-----------------------------------------------------------#
install_additional_packages

#-----------------------------------------------------------#
####  CUSTOMISE INSTALLED CONFIGURATION                 #####
#-----------------------------------------------------------#
updateInstalledConfigurationParams
update_website

#-----------------------------------------------------------#
####  START ORCHESTRATOR SERVICES                       #####
#-----------------------------------------------------------#
systemctl enable ${SERVICES_IDENTIFIER}
systemctl start ${SERVICES_IDENTIFIER}
systemctl enable ${EXECUTOR_SERVICE_IDENTIFIER}
systemctl start ${EXECUTOR_SERVICE_IDENTIFIER}
systemctl enable ${ORCHESTRATOR_SERVICE_IDENTIFIER}
systemctl start ${ORCHESTRATOR_SERVICE_IDENTIFIER}
systemctl enable ${SCHEDULER_SERVICE_IDENTIFIER}
systemctl start ${SCHEDULER_SERVICE_IDENTIFIER}
systemctl enable ${HTTP_LISTENER_SERVICE_IDENTIFIER}
systemctl start ${HTTP_LISTENER_SERVICE_IDENTIFIER}
systemctl enable ${MONITOR_AGENT_SERVICE_IDENTIFIER}
systemctl start ${MONITOR_AGENT_SERVICE_IDENTIFIER}

systemctl enable ${MONITOR_AGENT_SERVICE_IDENTIFIER}
systemctl start ${MONITOR_AGENT_SERVICE_IDENTIFIER}

systemctl enable sen2agri-fmask
systemctl enable sen2agri-fmask.timer
systemctl start sen2agri-fmask.timer

systemctl enable sen2agri-era5-downloader
systemctl enable sen2agri-era5-downloader.timer 
systemctl start sen2agri-era5-downloader
systemctl start sen2agri-era5-downloader.timer 

