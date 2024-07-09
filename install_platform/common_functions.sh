#!/bin/bash

: ${INSTAL_CONFIG_FILE:="./config/install_config.conf"}

: ${GPT_CONFIG_FILE:="./config/gpt.vmoptions"}

: ${SLURM_QOS_LIST:="qosMaccs,qosComposite,qosCropMask,qosCropType,qosPheno,qosLai,qoss4cmdb1,qoss4cl4a,qoss4cl4b,qoss4cl4c,qosfmask,qosvaliditymsk,qostrex,qosl3gencomp,qoss1comp,qoss4spermcrops,qoss4syield,qoss4scropmap,qoszarr,qoss4cheterog,qoss4cbaresoil,qoss4cchangedet"}

MAJA_VER="4.5.4"

ALL_CFG_VALUES=""

SERVICES_ARCHIVE=""
CONFIGURATION_NAME=""
PROJECT_NAME=""
SERVICES_CONFIGURATION_NAME="sen2agri"
CONFIGURATION_PROFILES=""
CONFIGURATION_DB_NAME=""
USE_SNAP="1"
INSTALL_CCI_LC="1"
DISABLED_PROCESSORS=()
DISABLED_CONFIG_CATEGORIES=()
DISABLED_PRODUCT_TYPE=()
USER_NAME=""
USER_PASS=""
USE_SEN2AGRI_IN_SERVICE_NAMES="0"

#------------------------------------------------------------------------------------------#
function join_by { local IFS="$1"; shift; echo "$*"; }

function load_configuration()
{
    if [ ! -f "${INSTAL_CONFIG_FILE}" ] ; then
        echo "The config file ${INSTAL_CONFIG_FILE} does not exist! Exiting ..."
        exit 1
    fi
    
    ALL_CFG_VALUES=($(awk '/\[/{prefix=$0; next} $1{print prefix $0}' ${INSTAL_CONFIG_FILE}))
    if [ ${#ALL_CFG_VALUES[@]} -eq 0 ] ; then
        echo "Couldn't load the config file ${INSTAL_CONFIG_FILE}! Exiting ..."
        exit 1
    fi
    
    # first get the GENERAL CONFIGURATION
    for element in "${ALL_CFG_VALUES[@]}"
    do
        if [[ $element == "[GENERAL]ACTIVE_CONFIGURATION"* ]] ; then
            ACTIVE_CONFIGURATION=$(cut -d "=" -f2 <<< "$element")
            if [ -z ${ACTIVE_CONFIGURATION} ] ; then 
                echo "No current configuration defined! Exiting ..."
                exit 1
            fi
        fi    
        if [[ $element == "[GENERAL]SUPPORTED_PROFILES"* ]] ; then
            CFG_SUPPORTED_PROFILES="$(cut -d "=" -f2 <<< "$element")"
            if [ -z ${CFG_SUPPORTED_PROFILES} ] ; then 
                echo "No supported profiles defined! Exiting ..."
                exit 1
            else
                IFS=', ' read -r -a SUPPORTED_PROFILES <<< "$CFG_SUPPORTED_PROFILES"
            fi
        fi
        
        if [[ $element == "[GENERAL]REQUIRED_PROCESSORS"* ]] ; then
            CFG_REQUIRED_PROCESSORS=$(cut -d "=" -f2 <<< "$element")
            if ! [[ -z $CFG_REQUIRED_PROCESSORS ]] ; then
                IFS=', ' read -r -a REQUIRED_PROCESSORS <<< "$CFG_REQUIRED_PROCESSORS"
            fi
        fi
        if [[ $element == "[GENERAL]REQUIRED_CONFIG_CATEGORIES"* ]] ; then
            CFG_REQUIRED_CONFIG_CATEGORIES=$(cut -d "=" -f2 <<< "$element") 
            if ! [[ -z $CFG_REQUIRED_CONFIG_CATEGORIES ]] ; then
                IFS=', ' read -r -a REQUIRED_CONFIG_CATEGORIES <<< "$CFG_REQUIRED_CONFIG_CATEGORIES"
            fi
        fi
        if [[ $element == "[GENERAL]REQUIRED_PRODUCT_TYPES"* ]] ; then
            CFG_REQUIRED_PRODUCT_TYPES=$(cut -d "=" -f2 <<< "$element")
            if ! [[ -z $CFG_REQUIRED_PRODUCT_TYPES ]] ; then
                IFS=', ' read -r -a REQUIRED_PRODUCT_TYPES <<< "$CFG_REQUIRED_PRODUCT_TYPES"
            fi
        fi
        
        if [[ $element == "[GENERAL]SERVICES_ARCHIVE"* ]] ; then
            SERVICES_ARCHIVE="$(cut -d "=" -f2 <<< "$element")"
        fi
        if [[ $element == "[GENERAL]INSTALL_SNAP"* ]] ; then
            USE_SNAP="$(cut -d "=" -f2 <<< "$element")"
        fi
        if [[ $element == "[GENERAL]INSTALL_CCI_LC"* ]] ; then
            INSTALL_CCI_LC="$(cut -d "=" -f2 <<< "$element")"
        fi
        if [[ $element == "[GENERAL]USE_SEN2AGRI_IN_SERVICE_NAMES"* ]] ; then
            USE_SEN2AGRI_IN_SERVICE_NAMES="$(cut -d "=" -f2 <<< "$element")"
        fi
        if [[ $element == "[GENERAL]PROJECT_NAME"* ]] ; then
            PROJECT_NAME="$(cut -d "=" -f2 <<< "$element")"
        fi
        
    done
    
    # now check the profile and the configuration keys from the active configuration 
    for element in "${ALL_CFG_VALUES[@]}"
    do
        if [[ $element == "[${ACTIVE_CONFIGURATION}]CONFIGURATION_NAME"* ]] ; then
            # just check that the current profile is correctly defined and supported
            CONFIGURATION_NAME=$(cut -d "=" -f2 <<< "$element")
            if [ -z ${CONFIGURATION_NAME} ] ; then
                echo "Name was not defined for the active configuration $ACTIVE_CONFIGURATION! Exiting ..."
                exit 1
            fi
        fi
        if [[ $element == "[${ACTIVE_CONFIGURATION}]DB_NAME"* ]] ; then
            CONFIGURATION_DB_NAME=$(cut -d "=" -f2 <<< "$element")
            if [ -z ${CONFIGURATION_DB_NAME} ] ; then
                echo "Database name was not defined for the active configuration $ACTIVE_CONFIGURATION! Exiting ..."
                exit 1
            fi
        fi
        
        if [[ $element == "[${ACTIVE_CONFIGURATION}]PROFILES"* ]] ; then
            CFG_PROFILES=$(cut -d "=" -f2 <<< "$element")
            if ! [[ -z $CFG_PROFILES ]] ; then
                IFS=', ' read -r -a CONFIGURATION_PROFILES <<< "$CFG_PROFILES"
                
                for profile in ${CONFIGURATION_PROFILES[@]} ; do
                    local in=1
                    for element in ${SUPPORTED_PROFILES[@]}; do
                        if [[ $element == "$profile" ]]; then
                            in=0
                            break
                        fi
                    done
                    if [ $in -eq 1 ] ; then
                        echo "Profile $profile defined in the active configuration $ACTIVE_CONFIGURATION is not supported. Exiting ..."
                        exit 1
                    fi
                done
            fi
        fi
        
        # Check if the services archive is overwritten for this configuration
        if [[ $element == "[${ACTIVE_CONFIGURATION}]SERVICES_ARCHIVE"* ]] ; then
            CFG_SERVICES_ARCHIVE=$(cut -d "=" -f2 <<< "$element")
            if ! [[ -z $CFG_SERVICES_ARCHIVE ]] ; then
                SERVICES_ARCHIVE="${CFG_SERVICES_ARCHIVE}"
            fi
        fi
        
        if [[ $element == "[${ACTIVE_CONFIGURATION}]REQUIRED_PROCESSORS"* ]] ; then
            CFG_REQUIRED_PROCESSORS=$(cut -d "=" -f2 <<< "$element")
            if ! [[ -z $CFG_REQUIRED_PROCESSORS ]] ; then
                IFS=', ' read -r -a REQUIRED_PROCESSORS1 <<< "$CFG_REQUIRED_PROCESSORS"
                REQUIRED_PROCESSORS+=(${REQUIRED_PROCESSORS1[@]})
            fi
        fi
        if [[ $element == "[${ACTIVE_CONFIGURATION}]REQUIRED_CONFIG_CATEGORIES"* ]] ; then
            CFG_REQUIRED_CONFIG_CATEGORIES=$(cut -d "=" -f2 <<< "$element") 
            if ! [[ -z $CFG_REQUIRED_CONFIG_CATEGORIES ]] ; then
                IFS=', ' read -r -a REQUIRED_CONFIG_CATEGORIES1 <<< "$CFG_REQUIRED_CONFIG_CATEGORIES"
                REQUIRED_CONFIG_CATEGORIES+=(${REQUIRED_CONFIG_CATEGORIES1[@]})
            fi
        fi
        if [[ $element == "[${ACTIVE_CONFIGURATION}]REQUIRED_PRODUCT_TYPES"* ]] ; then
            CFG_REQUIRED_PRODUCT_TYPES=$(cut -d "=" -f2 <<< "$element")
            if ! [[ -z $CFG_REQUIRED_PRODUCT_TYPES ]] ; then
                IFS=', ' read -r -a REQUIRED_PRODUCT_TYPES1 <<< "$CFG_REQUIRED_PRODUCT_TYPES"
                REQUIRED_PRODUCT_TYPES+=(${REQUIRED_PRODUCT_TYPES1[@]})
            fi
        fi
        if [[ $element == "[${ACTIVE_CONFIGURATION}]REQUIRED_AUXDATA_DESCRIPTORS"* ]] ; then
            CFG_REQUIRED_AUXDATA_DESCRIPTORS=$(cut -d "=" -f2 <<< "$element")
            if ! [[ -z $CFG_REQUIRED_AUXDATA_DESCRIPTORS ]] ; then
                IFS=', ' read -r -a REQUIRED_AUXDATA_DESCRIPTORS <<< "$CFG_REQUIRED_AUXDATA_DESCRIPTORS"
            fi
        fi
        
        if [[ $element == "[${ACTIVE_CONFIGURATION}]USER_NAME"* ]] ; then
            USER_NAME=$(cut -d "=" -f2 <<< "$element")
        fi
        if [[ $element == "[${ACTIVE_CONFIGURATION}]USER_PASS"* ]] ; then
            USER_PASS=$(cut -d "=" -f2 <<< "$element")
        fi
        if [[ $element == "[${ACTIVE_CONFIGURATION}]USER_MAIL"* ]] ; then
            USER_MAIL=$(cut -d "=" -f2 <<< "$element")
        fi
        
        if [[ $element == "[${ACTIVE_CONFIGURATION}]USE_SNAP"* ]] ; then
            USE_SNAP="$(cut -d "=" -f2 <<< "$element")"
        fi
        if [[ $element == "[${ACTIVE_CONFIGURATION}]INSTALL_CCI_LC"* ]] ; then
            INSTALL_CCI_LC="$(cut -d "=" -f2 <<< "$element")"
        fi
        if [[ $element == "[${ACTIVE_CONFIGURATION}]USE_SEN2AGRI_IN_SERVICE_NAMES"* ]] ; then
            USE_SEN2AGRI_IN_SERVICE_NAMES="$(cut -d "=" -f2 <<< "$element")"
        fi
        if [[ $element == "[${ACTIVE_CONFIGURATION}]PROJECT_NAME"* ]] ; then
            PROJECT_NAME="$(cut -d "=" -f2 <<< "$element")"
        fi
        
    done    
    
    # Check services archive name was defined
    if [ -z ${SERVICES_ARCHIVE} ] ; then
        echo "Services archive name was not defined! Exiting ..."
        exit 1
    fi

    # Check DB name was defined
    if [ -z ${CONFIGURATION_DB_NAME} ] ; then
        echo "Database name was not defined! Exiting ..."
        exit 1
    fi
    
    # Check configuration name was defined
    if [ -z ${CONFIGURATION_NAME} ] ; then
        echo "Configuration name was not defined! Exiting ..."
        exit 1
    fi

    # Check user name was defined
    if [ -z ${USER_NAME} ] ; then
        echo "User name was not defined! Exiting ..."
        exit 1
    fi

    # Check user name was defined
    if [ -z ${USER_PASS} ] ; then
        echo "User password was not defined! Exiting ..."
        exit 1
    fi

    if [ -z ${USER_MAIL} ] ; then
        echo "User email was not defined! Exiting ..."
        exit 1
    fi

    if [ -z ${PROJECT_NAME} ] ; then
        echo "Project name was not defined! Exiting ..."
        exit 1
    fi
    
    if [ "${CONFIGURATION_NAME}" != "sen2agri" ] ; then 
        if [ "${USE_SEN2AGRI_IN_SERVICE_NAMES}" == "0" ] ; then
            SERVICES_CONFIGURATION_NAME="${CONFIGURATION_NAME}"
            SERVICES_IDENTIFIER="${SERVICES_CONFIGURATION_NAME}-services"
            EXECUTOR_SERVICE_IDENTIFIER="${SERVICES_CONFIGURATION_NAME}-executor"
            EXECUTOR_TIMER_IDENTIFIER="${SERVICES_CONFIGURATION_NAME}-executor.timer"
            ORCHESTRATOR_SERVICE_IDENTIFIER="${SERVICES_CONFIGURATION_NAME}-orchestrator"
            SCHEDULER_SERVICE_IDENTIFIER="${SERVICES_CONFIGURATION_NAME}-scheduler"
            HTTP_LISTENER_SERVICE_IDENTIFIER="${SERVICES_CONFIGURATION_NAME}-http-listener"
            MONITOR_AGENT_SERVICE_IDENTIFIER="${SERVICES_CONFIGURATION_NAME}-monitor-agent"
        else 
            echo "Using sen2agri in services names"
        fi
    fi
    
    echo "SUPPORTED_PROFILES = ${SUPPORTED_PROFILES[@]}"
   
    echo "SERVICES_ARCHIVE = ${SERVICES_ARCHIVE}"
    echo "CONFIGURATION_NAME = ${CONFIGURATION_NAME}"
    echo "USE_SEN2AGRI_IN_SERVICE_NAMES = ${USE_SEN2AGRI_IN_SERVICE_NAMES}"
    echo "SERVICES_CONFIGURATION_NAME = ${SERVICES_CONFIGURATION_NAME}"
    echo "CONFIGURATION_PROFILES = ${CONFIGURATION_PROFILES[@]}"
    echo "CONFIGURATION_DB_NAME = ${CONFIGURATION_DB_NAME}"
    echo "REQUIRED_PROCESSORS = ${REQUIRED_PROCESSORS[@]}"
    echo "REQUIRED_CONFIG_CATEGORIES = ${REQUIRED_CONFIG_CATEGORIES[@]}"
    echo "REQUIRED_PRODUCT_TYPES = ${REQUIRED_PRODUCT_TYPES[@]}"
    echo "REQUIRED_AUXDATA_DESCRIPTORS = ${REQUIRED_AUXDATA_DESCRIPTORS[@]}"
    echo "USER_NAME = ${USER_NAME}"
    echo "USER_PASS = ${USER_PASS}"
    echo "PROJECT_NAME = ${PROJECT_NAME}"
}

function filter_required_components()
{
    PROCS_TO_KEEP=$(join_by , "${REQUIRED_PROCESSORS[@]}")
    AUXDATA_DESCRS_TO_KEEP=$(join_by , "${REQUIRED_AUXDATA_DESCRIPTORS[@]}")
    CONFIG_CATEG_TO_KEEP=$(join_by , "${REQUIRED_CONFIG_CATEGORIES[@]}")
    PRD_TYPES_TO_KEEP=$(join_by , "${REQUIRED_PRODUCT_TYPES[@]}")
    
    # First get the processors to remove
    PROCS_TO_REMOVE=`psql -X -A -U admin ${CONFIGURATION_DB_NAME} -t -c "select string_agg(id::text, ',' ORDER BY id ASC) from processor where id NOT IN (${PROCS_TO_KEEP})"`
    AUX_DESCRS_TO_REMOVE=`psql -X -A -U admin ${CONFIGURATION_DB_NAME} -t -c "select string_agg(id::text, ',' ORDER BY id ASC) from auxdata_descriptor where id NOT IN (${AUXDATA_DESCRS_TO_KEEP})"`
    CFG_CATEG_TO_REMOVE=`psql -X -A -U admin ${CONFIGURATION_DB_NAME} -t -c "select string_agg(id::text, ',' ORDER BY id ASC) from config_category where id NOT IN (${CONFIG_CATEG_TO_KEEP})"`
    PRD_TYPES_TO_REMOVE=`psql -X -A -U admin ${CONFIGURATION_DB_NAME} -t -c "select string_agg(id::text, ',' ORDER BY id ASC) from product_type where id NOT IN (${PRD_TYPES_TO_KEEP})"`
    
    IFS=', ' read -r -a PROCS_TO_REMOVE_ARR <<< "$PROCS_TO_REMOVE"
    IFS=', ' read -r -a AUX_DESCRS_TO_REMOVE_ARR <<< "$AUX_DESCRS_TO_REMOVE"
    IFS=', ' read -r -a CFG_CATEG_TO_REMOVE_ARR <<< "$CFG_CATEG_TO_REMOVE"
    IFS=', ' read -r -a PRD_TYPES_TO_REMOVE_ARR <<< "$PRD_TYPES_TO_REMOVE"

    # Delete the auxdata_xxxx entries     
    if ! [[ ${#AUX_DESCRS_TO_REMOVE_ARR[@]} -eq 0 ]] ; then
        echo "Deleting the auxdata info with IDs = $AUX_DESCRS_TO_REMOVE"
        psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from auxdata_operation WHERE auxdata_file_id IN (SELECT id from auxdata_file WHERE auxdata_descriptor_id IN ($AUX_DESCRS_TO_REMOVE))"
        psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from auxdata_file WHERE auxdata_descriptor_id IN ($AUX_DESCRS_TO_REMOVE)"
        psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from auxdata_descriptor WHERE id IN ($AUX_DESCRS_TO_REMOVE)"
    fi

    # Delete the processors that are not in the list of required ones
    if ! [[ ${#PROCS_TO_REMOVE_ARR[@]} -eq 0 ]] ; then
        for PROC_ID in ${PROCS_TO_REMOVE_ARR[@]} ; do 
            PROC_SHORT_NAME=`psql -X -A -U admin ${CONFIGURATION_DB_NAME} -t -c "select short_name from processor where id = ${PROC_ID}"`
            echo "Clearing config values for processor with ID = $PROC_ID and short name = $PROC_SHORT_NAME ..."
            if [ ! -z ${PROC_SHORT_NAME} ] ; then 
                PROC_PREFIX="%processor.${PROC_SHORT_NAME}.%"
                echo "Executing command : delete from config WHERE LOWER(key) like '${PROC_PREFIX}'"
                psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from config WHERE LOWER(key) like '${PROC_PREFIX}'"
                psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from config_metadata WHERE LOWER(key) like '${PROC_PREFIX}'"
            fi
        done 

        echo "Disabling processors with IDs = $PROCS_TO_REMOVE"
        psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from default_scheduled_tasks where processor_id IN ($PROCS_TO_REMOVE)"
        psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from processor WHERE id IN ($PROCS_TO_REMOVE)"
        psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from default_scheduled_tasks WHERE processor_id IN ($PROCS_TO_REMOVE)"
    fi

    # Delete the config categories
    if ! [[ ${#CFG_CATEG_TO_REMOVE_ARR[@]} -eq 0 ]] ; then
        echo "Disabling config categories with IDs = $CFG_CATEG_TO_REMOVE"
        # psql -U postgres ${CONFIGURATION_DB_NAME} -c "UPDATE config_category SET available = 'false' WHERE id IN ($CFG_CATEG_TO_REMOVE)"
        psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from config_category WHERE id IN ($CFG_CATEG_TO_REMOVE)"
        psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from config where key in (select key from config_metadata where config_category_id IN ($CFG_CATEG_TO_REMOVE))"
        psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from config_metadata where config_category_id IN ($CFG_CATEG_TO_REMOVE)"
    fi
    
    # Delete the product types
    if ! [[ ${#PRD_TYPES_TO_REMOVE[@]} -eq 0 ]] ; then
        echo "Disabling product types with IDs = $PRD_TYPES_TO_REMOVE"
        # psql -U postgres ${CONFIGURATION_DB_NAME} -c "UPDATE product_type SET available = 'false' WHERE id IN ($PRD_TYPES_TO_REMOVE)"
        psql -U postgres ${CONFIGURATION_DB_NAME} -c "delete from product_type WHERE id IN ($PRD_TYPES_TO_REMOVE)"
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
