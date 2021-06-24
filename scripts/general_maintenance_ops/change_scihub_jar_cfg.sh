#!/bin/bash

DATA_SOURCE="apihub"

function usage() {
    echo "Usage: -s <DATA_SOURCE - (apihub|dhus)>"
    exit 1
}


POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -s|--data-source)
    DATA_SOURCE="$2"
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

if [ -z ${DATA_SOURCE} ] ; then
    echo "No datasource provided!"
    usage
fi 

if [[ "${DATA_SOURCE}" != "apihub" && "${DATA_SOURCE}" != "dhus" ]] ; then
    echo "Please provide apihub or dhus!"
    usage
fi

echo "Copying /usr/share/sen2agri/sen2agri-services/lib/tao-datasources-scihub-*.jar locally ..."
cp -fr /usr/share/sen2agri/sen2agri-services/lib/tao-datasources-scihub-*.jar .

echo "Unzipping ro/cs/tao/datasource/remote/scihub/scihub.properties ..."
unzip -o tao-datasources-scihub-*.jar ro/cs/tao/datasource/remote/scihub/scihub.properties

if [ "${DATA_SOURCE}" == "apihub" ] ; then
    echo "Uncommenting apihub line ..."
    # updating also for the new address in SciHub
    sed -i 's/^#scihub.search.url = https:\/\/scihub.copernicus.eu\/apihub\/search/scihub.search.url = https:\/\/apihub.copernicus.eu\/apihub\/search/g' ro/cs/tao/datasource/remote/scihub/scihub.properties
    sed -i 's/^#scihub.search.url = https:\/\/apihub.copernicus.eu\/apihub\/search/scihub.search.url = https:\/\/apihub.copernicus.eu\/apihub\/search/g' ro/cs/tao/datasource/remote/scihub/scihub.properties

    echo "Commenting dhus line ..."
    sed -i 's/^scihub.search.url = https:\/\/scihub.copernicus.eu\/dhus\/search/#scihub.search.url = https:\/\/scihub.copernicus.eu\/dhus\/search/g' ro/cs/tao/datasource/remote/scihub/scihub.properties
else 
    echo "Uncommenting dhus line ..."
    sed -i 's/^#scihub.search.url = https:\/\/scihub.copernicus.eu\/dhus\/search/scihub.search.url = https:\/\/scihub.copernicus.eu\/dhus\/search/g' ro/cs/tao/datasource/remote/scihub/scihub.properties

    echo "Commenting apihub line ..."
    sed -i 's/^scihub.search.url = https:\/\/scihub.copernicus.eu\/apihub\/search/#scihub.search.url = https:\/\/scihub.copernicus.eu\/apihub\/search/g' ro/cs/tao/datasource/remote/scihub/scihub.properties
    #update for the new address in SciHub
    sed -i 's/^scihub.search.url = https:\/\/apihub.copernicus.eu\/apihub\/search/#scihub.search.url = https:\/\/apihub.copernicus.eu\/apihub\/search/g' ro/cs/tao/datasource/remote/scihub/scihub.properties

fi 
echo "Putting scihub.properties back to jar ..."
zip -f tao-datasources-scihub-*.jar  ro/cs/tao/datasource/remote/scihub/scihub.properties

echo "Copying tao-datasources-scihub-*.jar back to /usr/share/sen2agri/sen2agri-services/lib/ ..."
sudo cp -fr tao-datasources-scihub-*.jar /usr/share/sen2agri/sen2agri-services/lib/

echo "Restarting sen2agri-services ..."
sudo systemctl restart sen2agri-services

echo "Done!"