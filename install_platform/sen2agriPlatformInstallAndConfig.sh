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
## sudo ./sen2agriPlatormInstallAndConfig.sh
################################################################################################
: ${INSTAL_CONFIG_FILE:="./config/install_config.conf"}
: ${GPT_CONFIG_FILE:="./config/gpt.vmoptions"}
#-----------------------------------------------------------------------------------------#
: ${SYS_ACC_NAME:="sen2agri-service"}
: ${SLURM_ACC_NAME:="slurm"}
: ${MUNGE_ACC_NAME:="munge"}
#-----------------------------------------------------------------------------------------#
: ${SLURM_CONF_PATH:="/etc/slurm"}
: ${SLURM_CLUSTER_NAME:="sen2agri"}
: ${SLURM_MACHINE_NOCPUS:=$(cat /proc/cpuinfo | grep processor | wc -l)}
: ${SLURM_CONFIG:="slurm.conf"}
: ${SLURM_CONFIG_DB:="slurmdbd.conf"}
: ${SLURM_QOS_LIST:="qosMaccs,qosComposite,qosCropMask,qosCropType,qosPheno,qosLai,qoss4cmdb1,qoss4cl4a,qoss4cl4b,qoss4cl4c,qosfmask,qosvaliditymsk,qostrex"}
#----------------SLURM MYSQL DATABASE CREATION---------------------------------------------#
MYSQL_DB_CREATION="create database slurm_acct_db;create user slurm@localhost;
set password for slurm@localhost = password('sen2agri');"
MYSQL_DB_ACCESS_GRANT="grant usage on *.* to slurm;grant all privileges on slurm_acct_db.* to slurm;flush privileges;"
MYSQL_CMD=${MYSQL_DB_CREATION}${MYSQL_DB_ACCESS_GRANT}
#----------------SEN2AGRI POSTGRESQL DATABASE NAME-----------------------------------------#
: ${SEN2AGRI_DATABASE_NAME:="sen2agri"}
#------------------------------------------------------------------------------------------#
declare -r -i -g L1C_PROCESSOR_SEN2COR=1
declare -r -i -g L1C_PROCESSOR_MAJA=2
#------------------------------------------------------------------------------------------#
function get_install_config_property
{
    grep "^$1=" "${INSTAL_CONFIG_FILE}" | cut -d'=' -f2 | sed -e 's/\r//g'
}

#-----------------------------------------------------------#

function parse_and_update_slurm_conf_file()
{
   ####################################
   ####  copy conf files to /etc/slurm
   ####################################
   mkdir -p ${SLURM_CONF_PATH}
   cp $(find ./ -name ${SLURM_CONFIG}) ${SLURM_CONF_PATH}
   cp $(find ./ -name ${SLURM_CONFIG_DB}) ${SLURM_CONF_PATH}
   chown slurm:slurm ${SLURM_CONF_PATH}/${SLURM_CONFIG_DB}
   chmod 600 ${SLURM_CONF_PATH}/${SLURM_CONFIG_DB}

   sed -ri "s|CPUs=.+|CPUs=${SLURM_MACHINE_NOCPUS}|g" /etc/slurm/slurm.conf
}
#-----------------------------------------------------------#
function create_slurm_data_base()
{
   ##install expect
   yum -y install expect expectk

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
   systemctl start slurm

   ##enable slurm service to start at boot
   systemctl enable slurm

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
   usermod -aG dockerroot ${SYS_ACC_NAME}
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
    docker run --rm -u $(id -u $SYS_ACC_NAME):$(id -g $SYS_ACC_NAME) -v /etc/sen2agri/sen2agri.conf:/etc/sen2agri/sen2agri.conf -v /var/lib/t-rex:/var/lib/t-rex sen4cap/data-preparation:0.1 t-rex-genconfig.py --stub /var/lib/t-rex/t-rex.toml

    cd docker
    docker-compose up -d

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
}
#-----------------------------------------------------------#
function install_and_config_postgresql()
{
   # NB: the container uses `trust`, not `peer` for local connections

   # Install `psql` and client libraries
   yum -y install postgresql12

   #------------DATABASE CREATION------------#
    #DB_NAME=$(head -q -n 1 ./config/db_name.conf 2>/dev/null)
    DB_NAME=$(get_install_config_property "DB_NAME")
    if [ -z "$DB_NAME" ]; then
        DB_NAME="sen2agri"
    fi
    SEN2AGRI_DATABASE_NAME=${DB_NAME}

    if ! [[ "${SEN2AGRI_DATABASE_NAME}" == "sen2agri" ]] ; then
        echo "Using database '${SEN2AGRI_DATABASE_NAME}'"
        sed -i -- "s/-- DataBase Create: sen2agri/-- DataBase Create: ${SEN2AGRI_DATABASE_NAME}/g" ./database/00-database/sen2agri.sql
        sed -i -- "s/CREATE DATABASE sen2agri/CREATE DATABASE ${SEN2AGRI_DATABASE_NAME}/g" ./database/00-database/sen2agri.sql
        sed -i -- "s/-- Privileges: sen2agri/-- Privileges: ${SEN2AGRI_DATABASE_NAME}/g" ./database/09-privileges/privileges.sql
        sed -i -- "s/GRANT ALL PRIVILEGES ON DATABASE sen2agri/GRANT ALL PRIVILEGES ON DATABASE ${SEN2AGRI_DATABASE_NAME}/g" ./database/09-privileges/privileges.sql
    fi

   # first, the database is created. the privileges will be set after all
   # the tables, data and other stuff is created (see down, privileges.sql
   cat "$(find ./ -name "database")/00-database"/sen2agri.sql | psql -U postgres


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
}
#-----------------------------------------------------------#
function populate_from_scripts()
{
   local curPath=$1
   local customDbPath=${curPath}/${SEN2AGRI_DATABASE_NAME}
   #for each sql scripts found in this folder
   for scriptName in "$curPath"/*.sql
      do
        scriptToExecute=${scriptName}
        if [ -d "$customDbPath" ]; then
            scriptFileName=$(basename -- "$scriptName")

            if [ -f ${customDbPath}/${scriptFileName} ]; then
                scriptToExecute=${customDbPath}/${scriptFileName}
            fi
        fi
         ## perform execution of each sql script
         echo "Executing SQL script: $scriptToExecute"
         cat "$scriptToExecute" | psql -U postgres "${SEN2AGRI_DATABASE_NAME}"
      done
    # Now check for the custom db folder if there are new scripts, others than in sen2agri. In this case, we must execute them too
    if [ -d "$customDbPath" ]; then
        for scriptName in "$customDbPath"/*.sql
        do
            scriptToExecute=${scriptName}
            scriptFileName=$(basename -- "$scriptName")
            if [[ ! -f ${curPath}/${scriptFileName} ]]; then
                ## perform execution of each sql script
                echo "Executing SQL script: $scriptToExecute"
                cat "$scriptToExecute" | psql -U postgres "${SEN2AGRI_DATABASE_NAME}"
            fi
        done
    fi
}
#-----------------------------------------------------------#
function install_downloaders_demmacs()
{
   mkdir /var/log/sen2agri
   chown ${SYS_ACC_NAME}: /var/log/sen2agri

   ##install prerequisites for Downloaders
   yum -y install wget python-lxml bzip2 python-beautifulsoup4 python-dateutil java-1.8.0-openjdk

   ##install Sen2Agri Downloaders  & Demmacs
   yum -y install ../rpm_binaries/sen2agri-downloaders-demmaccs-*.centos7.x86_64.rpm

   ldconfig

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
   yum -y install gdal-python python-psycopg2 python-dateutil gd

   ##install gdal 2.3 from the local repository
   yum -y install ../rpm_binaries/gdal-local-*.centos7.x86_64.rpm

   # Some GDAL Python tools will pick the wrong GDAL version, but
   # we don't need them
   rm -f /usr/local/bin/gdal_edit.py

   ##install Orfeo ToolBox
   yum -y install ../rpm_binaries/otb-*.rpm

   ##install Sen2Agri Processors
   yum -y install ../rpm_binaries/sen2agri-processors-*.centos7.x86_64.rpm

   ln -s /usr/lib64/libproj.so.0 /usr/lib64/libproj.so
   ldconfig

   ##install Sen2Agri Services
   yum -y install ../rpm_binaries/sen2agri-app-*.centos7.x86_64.rpm

   ##install SLURM
   yum -y install slurm slurm-slurmctld slurm-slurmd slurm-devel slurm-pam_slurm slurm-perlapi slurm-slurmdbd slurm-torque slurm-libs
}

#-----------------------------------------------------------#
function install_additional_packages()
{
    if ! [[ "${SEN2AGRI_DATABASE_NAME}" == "sen2agri" ]] ; then
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
        if [ -d $l1c_processor_gipp_source ]; then
            echo "Copying $l1c_processor_name GIPP files to ${l1c_processor_gipp_destination}"
            mkdir -p ${l1c_processor_gipp_destination}
            cp -rf "${l1c_processor_gipp_source}"/* ${l1c_processor_gipp_destination}

            echo "Copying UserConfiguration into maja gipp location ..."
            cp -fR ./config/maja/UserConfiguration ${l1c_processor_gipp_destination}

        else
            echo "Cannot find $l1c_processor_name GIPP files in the distribution, please copy them to $l1c_processor_gipp_destination"
        fi
    fi

    echo "Creating /mnt/archive/reference_data"
    mkdir -p /mnt/archive/reference_data
    echo "Copying reference data"
    if [ -d ../reference_data/ ]; then
        cp -rf ../reference_data/* /mnt/archive/reference_data
    fi
}

# Update /etc/sen2agri/sen2agri.conf with the right database
function updateSen2AgriProcessorsParams()
{
    DB_NAME=$(get_install_config_property "DB_NAME")
    if [[ ! -z $DB_NAME ]] ; then
        sed -i -e "s|DatabaseName=sen2agri|DatabaseName=$DB_NAME|g" /etc/sen2agri/sen2agri.conf
    fi
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

    if [ -z ${zipArchive} ] ; then
        echo "No sen2agri-services zip archive provided in ../sen2agri-services"
        echo "Exiting now"
        exit 1
    else
        filename="${zipArchive%.*}"

        echo "Extracting into /usr/share/sen2agri/sen2agri-services from archive $zipArchive ..."

        mkdir -p /usr/share/sen2agri/sen2agri-services && unzip ${zipArchive} -d /usr/share/sen2agri/sen2agri-services
        if [ $? -ne 0 ]; then
            echo "Unable to unpack the sen2agri-services into/usr/share/sen2agri/sen2agri-services"
            echo "Exiting now"
            exit 1
        fi
        # convert any possible CRLF into LF
        tr -d '\r' < /usr/share/sen2agri/sen2agri-services/bin/start.sh > /usr/share/sen2agri/sen2agri-services/bin/start.sh.tmp && cp -f /usr/share/sen2agri/sen2agri-services/bin/start.sh.tmp /usr/share/sen2agri/sen2agri-services/bin/start.sh && rm /usr/share/sen2agri/sen2agri-services/bin/start.sh.tmp
        # ensure the execution flag
        chmod a+x /usr/share/sen2agri/sen2agri-services/bin/start.sh

        # it might happen that some files to be packaged with the wrong read rights
        chmod -R a+r /usr/share/sen2agri/sen2agri-services

        # update the database name if needed in the sen2agri-services
        DB_NAME=$(get_install_config_property "DB_NAME")
        if [[ ! -z $DB_NAME ]] ; then
            sed -i -e "s/sen2agri?stringtype=unspecified/${DB_NAME}?stringtype=unspecified/" /usr/share/sen2agri/sen2agri-services/config/services.properties
        fi
    fi
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

#use MACCS or MAJA?
maccs_or_maja

check_paths

disable_selinux
disable_firewall

##install EPEL for dependencies, PGDG for the Postgres client libraries and
yum -y install epel-release https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm yum-utils
yum -y update epel-release pgdg-redhat-repo
yum-config-manager --disable pgdg95
yum -y install docker docker-compose gdal jq
sed -i "s/'--selinux-enabled /'/" /etc/sysconfig/docker

jq '. + { group: "dockerroot" }' < /etc/docker/daemon.json > /etc/docker/daemon.json.new
mv -f /etc/docker/daemon.json.new /etc/docker/daemon.json

systemctl enable docker
systemctl restart docker

install_sen2agri_services

#-----------------------------------------------------------#
####  OTB, SEN2AGRI, SLURM INSTALL  & CONFIG     ######
#-----------------------------------------------------------#
## install binaries
install_RPMs
updateSen2AgriProcessorsParams

## create system account
create_system_account

## config and start munge
config_and_start_munge_service

## config and start slurm
config_and_start_slurm_service

config_docker

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
####  START ORCHESTRATOR SERVICES                       #####
#-----------------------------------------------------------#
systemctl enable sen2agri-services
systemctl start sen2agri-services
systemctl enable sen2agri-executor
systemctl start sen2agri-executor
systemctl enable sen2agri-orchestrator
systemctl start sen2agri-orchestrator
systemctl enable sen2agri-scheduler
systemctl start sen2agri-scheduler
systemctl enable sen2agri-http-listener
systemctl start sen2agri-http-listener
systemctl enable sen2agri-monitor-agent
systemctl start sen2agri-monitor-agent


