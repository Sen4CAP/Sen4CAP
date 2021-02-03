# Known issues
 - The SAFE and L2A product previews are not gamma-corrected and can be too dark
 - The SAFE validation step flags as invalid products with even a single tile having a low-variance band as invalid (`NOTV`)
 - The website has display issues on Safari
 - When deleting a site, the folders for the L2A products that were not processed by MACCS are not deleted and should be deleted manually. Normally these folders contain no valid product and contain only log and EEF files.
 - Even if in sen4cap conda file is specified psycopg2 as pip package to be installed, it is not always installed. This makes L4B processor to fail. A manual installation might be needed using :
    sudo su -l sen2agri-service
    conda activate sen4cap
    pip install psycopg2
 - When executing seasons from the past, the automatic scheduled jobs might be executed before having all the products pre-processed
 - The L4C jobs are marked as finished with success even if there were no products as input.
 - MAJA still needs to be installed even when using only ESA L2A products from SciHub
 - The old marker products are not overwritten
 - The L4A markers are computed only when the L4A processor runs
 - New L4A markers are added for custom jobs
 - The HTTP API only returns markers from the most recent product

# Change Log

## [2.0]
### Added
- The tillage processor
- Docker containerization for sen2agri-processors and MAJA
- Marker database and REST API for accessing it
- Possibility to use MAJA 4 or Sen2Cor for L2A pre-processing
- Posibility to use SNAP 8 in a docker container and use AsterDEM for nordic countries where SRTM is not available
- Option to use another DEM for MAJA L2A pre-processing

### Changed
- Communication protocol between orchestrator components (HTTP instead of DBus)
- The L4A processor loads the data faster (by using Arrow IPC instead of CSV files)

### Fixed
- Some issues related to the L4B scripts due to some changes in the conda python gdal bindings
- Some DB concurrency fixes for the L2A launcher script

## [1.3]
### Added
- Add buffer (in days) for queries (last query date - no.days)
- Retriable queries (json persistence)
- Support for HTTPS in sen4cap services (disabled by default)
- Optimisation of thread usage when fetch_mode is SymLink or Check
- Import: products filtering by intersection with the respective site
- L2 S1 product metadata: added original extent and projection code
- More details in database for S1 SLC pairs pre-processing
- An extra check before exporting L4C tables that the l4c_practices exists (avoid unnecessary error messages)
- Updated feature extraction to use just the main bands in the S2-only mode and include the red edge bands when both S1 and S2 are requested, to avoid resampling 20m bands to 10m

### Changed
- SLURM service start type was changed from forking to simple
- USGS API updates

### Fixed
- Do not continue processing S1 products if the site gets disabled
- Save WGS84 extent in database for S1 L2 products
- CreoDIAS data source mixed geometries fix (polygon/multipolygon)
- Corrections for naming of L2 S1 products when only AMP or COHE pre-processing is enabled
- L4B Corrections for the bug when some users were obtaining the error "CreateSpatialIndex : unsupported operation on a read-only datasource"
- L4B Corrections for encoding issues (special characters not correctly translated into the output product)
- On LPIS products in WGS84 sometimes some indicators were not computed correctly and the execution could have crashed due to the invalid geometry
- L4A - fixed classification script crash in the presence of updated R dependencies
- L4A - fixed classification script crash on some input data
- L4A - fixed possible crash when no S1 data is available


## [1.2]
### Added
  - A mechanism for dynamic timeout computation when performing queries from SciHub (due to large delays for S1 queries experienced during maintenance activities)
  - The configuration keys (in config table) downloader.s1.query.days.back, downloader.s2.query.days.back and downloader.l8.query.days.back (default 0) to force datasources query products from the last product date minus given value in these keys (for products that might occur with delay of several days on SciHub)

### Changed
  - The processors jobs will fail and be stopped if one of the steps is failing

### Fixed
  - CropType processor according to https://forum.esa-sen4cap.org/t/important-correction-to-the-l4a-crop-type-processor/111
  - After upgrade to version 1.1 from 1.0 the package php-pgsql might be uninstalled by mistake, causing the login into the website to fail. The package will be installed during upgrading, if missing.

## [1.1]
### Added
  - Script for importing THEIA products
  - Support for using download and usage of Sen2Cor L2A products (configurable via database)
  - Reports and statistics visualisation for the downloaded and pre-processed products

### Changed
  - The MAJA training interval was changed to 2 months instead of 3 months
  - The icons in the output of the jobs from the "monitoring" tab were updated to reflect more step states.
  - The Postgres server is running now in a Docker container.

### Fixed
  - Updates for the new changes in USGS API
  - Corrections for avoiding skipping one product per page in SciHub
  - Various corrections in LPIS import module and L4A, L4B and L4C processors.
  - Corrections in the installation and upgrade scripts
  - Correction in the S1 preview in the web interface
  - Fixes in the web interface for some issues in MS Edge browser
  - Datasource configuration changes do not require any more to restart the services
  - Various ABI versioning issues that prevented the system from working after installing some system updates and/or lead to crashes in the L4C processor.

## Compatibility warning

  - The next release will use a newer version of MAJA, which will not be able to work with products from previous versions. If you're using MAJA, you will need to restart the processing of your site from scratch.

# Change Log

## [1.0.0]

### Added
  - Web interface for uploading LPIS/Declaration
  - Web interface for uploading configuration and files for L4B and L4C

