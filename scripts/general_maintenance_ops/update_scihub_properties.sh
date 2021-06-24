#!/bin/bash

echo "Copying /usr/share/sen2agri/sen2agri-services/lib/tao-datasources-scihub-*.jar locally ..."
cp -fr /usr/share/sen2agri/sen2agri-services/lib/tao-datasources-scihub-*.jar .

echo "Unzipping ro/cs/tao/datasource/remote/scihub/scihub.properties ..."
unzip -o tao-datasources-scihub-*.jar ro/cs/tao/datasource/remote/scihub/scihub.properties

echo "Updating scihub.apihub.url ..."
sed -i 's/scihub.apihub.url = https:\/\/scihub.copernicus.eu\/apihub\/search/scihub.apihub.url = https:\/\/apihub.copernicus.eu\/apihub\/search/g' ro/cs/tao/datasource/remote/scihub/scihub.properties
sed -i 's/scihub.search.url = https:\/\/scihub.copernicus.eu\/apihub\/search/scihub.search.url = https:\/\/apihub.copernicus.eu\/apihub\/search/g' ro/cs/tao/datasource/remote/scihub/scihub.properties

echo "Updating scihub.search.count.url ..."
sed -i 's/scihub.search.count.url = https:\/\/scihub.copernicus.eu\/apihub\/search/scihub.search.count.url = https:\/\/apihub.copernicus.eu\/apihub\/search/g' ro/cs/tao/datasource/remote/scihub/scihub.properties

echo "Updating scihub.product.url ..."
sed -i 's/scihub.product.url = https:\/\/scihub.copernicus.eu\/apihub\/odata\/v1/scihub.product.url = https:\/\/apihub.copernicus.eu\/apihub\/odata\/v1/g' ro/cs/tao/datasource/remote/scihub/scihub.properties

echo "Putting scihub.properties back to jar ..."
zip -f tao-datasources-scihub-*.jar  ro/cs/tao/datasource/remote/scihub/scihub.properties

echo "Copying tao-datasources-scihub-*.jar back to /usr/share/sen2agri/sen2agri-services/lib/ ..."
sudo cp -fr tao-datasources-scihub-*.jar /usr/share/sen2agri/sen2agri-services/lib/

echo "Restarting sen2agri-services ..."
sudo systemctl restart sen2agri-services

echo "Done!"