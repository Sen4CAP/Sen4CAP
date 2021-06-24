#!/bin/bash

function usage() {
    echo "Usage: ./s2_site_force_download_restart.sh -s <site_short_name> -d <dbname> --s1 <1 or 0> --s2 <1 or 0>"
    exit 1
}


reset_s1="0"
reset_s2="0"

database=`(grep -w "DatabaseName" | cut -d= -f2) </etc/sen2agri/sen2agri.conf`
if [ -z $database ] ; then
    database="sen4cap"
fi

function print_sites() {
    psql -U admin $database -c "select id, name, short_name from site;"
    exit 1
}

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -p|--print-sites)
    print_sites
    ;;

    -s|--site)
    site_name="$2"
    shift # past argument
    shift # past value
    ;;
    -d|--db)
    database="$2"
    shift # past argument
    shift # past value
    ;;

    --s1)
    reset_s1="$2"
    shift # past argument
    shift # past value
    ;;

    --s2)
    reset_s2="$2"
    shift # past argument
    shift # past value
    ;;

    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [ -z ${site_name} ] ; then
    echo "No site_name provided!" && usage
fi

echo "Using database $database ..."

if [ "${site_name}" == "all_sites" ] ; then
    if [ "$reset_s1" == "1" ] ; then 
        echo "Setting S1 force start for all sites ..."
        psql -U admin $database -c "INSERT INTO config(key, site_id, value) VALUES ('downloader.S1.forcestart', null, true) on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';"
        psql -U admin $database -c "update config set value = true where key = 'downloader.S1.forcestart';"
    fi
    if [ "$reset_s2" == "1" ] ; then 
        echo "Setting S2 force start for all sites ..."
        psql -U admin $database -c "INSERT INTO config(key, site_id, value) VALUES ('downloader.S2.forcestart', null, true) on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';"
        psql -U admin $database -c "update config set value = true where key = 'downloader.S2.forcestart';"
    fi
    
else
    if [ "$(psql -qtAX -U postgres $database -c "select exists(select from site where short_name='$site_name')")" != t ] ; then
        echo "The site with the name $site_name does not exists. Please choose a valid site!"
        exit 1
    fi

    site_id="$(psql -qtAX -U postgres $database -c "select id from site where short_name='$site_name'")"
    echo "Site id for $site_name is $site_id"

    if [ "$reset_s1" == "1" ] ; then 
        echo "Setting S1 force start for site ${short_name} ..."
        psql -U admin $database -c "INSERT INTO config(key, site_id, value) VALUES ('downloader.S1.forcestart', ${site_id}, true) on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';"
    fi
    if [ "$reset_s2" == "1" ] ; then 
        echo "Setting S2 force start for site ${short_name} ..."
        psql -U admin $database -c "INSERT INTO config(key, site_id, value) VALUES ('downloader.S2.forcestart', ${site_id}, true) on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';"
    fi
fi

sudo systemctl restart sen2agri-services