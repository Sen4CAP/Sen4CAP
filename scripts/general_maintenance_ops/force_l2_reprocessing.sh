#!/bin/bash

function usage() {
    echo "Usage: ./force_l2a_reprocessing.sh -p <product_name>"
    exit 1
}

database=`(grep -w "DatabaseName" | cut -d= -f2) </etc/sen2agri/sen2agri.conf`
if [ -z $database ] ; then
    database="sen4cap"
fi

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -p|--product)
    product_name="$2"
    shift # past argument
    shift # past value
    ;;
    -d|--db)
    database="$2"
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

if [ -z ${product_name} ] ; then
    echo "No product provided!" && usage
fi

echo "Using database $database ..."


if echo "${product_name}" | grep -q "MSIL2A"; then
    origprd=${product_name}
    product_name=${product_name/MSIL2A/MSIL1C}
    echo "Using L1C product name $product_name derived from $origprd ..."
fi

echo "Product name to be reset is : $product_name"

psql -U admin $database -c "delete from l1_tile_history where downloader_history_id in (select id from downloader_history where product_name = '$product_name');"
psql -U admin $database -c "update downloader_history set status_id = 2 where  product_name= '$product_name');"