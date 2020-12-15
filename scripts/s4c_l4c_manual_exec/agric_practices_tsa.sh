#!/bin/bash

function usage() {
    echo "Usage: ./agric_practices_tsa.sh -c <COUNTRY_CODE - (NLD|CZE|LTU|ESP|ITA|ROU)> -y <YEAR>"
    exit 1
}
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -c|--country)
    COUNTRY_AND_REGION="$2"
    shift # past argument
    shift # past value
    ;;
    -y|--year)
    YEAR="$2"
    shift # past argument
    shift # past value
    ;;
    -d|--prod-date)
    PROD_DATE="$2"
    shift # past argument
    shift # past value
    ;;
    -p|--prev-product)
    PREV_PRODUCT="$2"
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

echo COUNTRY        = "${COUNTRY_AND_REGION}"
echo YEAR           = "${YEAR}"

if [ -z ${COUNTRY_AND_REGION} ] ; then
    echo "No country provided!"
    usage
fi 

if [ -z ${YEAR} ] ; then
    echo "No year provided!"
    usage
fi 

if [ -z ${PROD_DATE} ] ; then
    echo "No product date provided. End of year limit date will be considered for gaps!"
    ACQ_DATE_LIMIT_OPT=""
else 
    #PROD_DATE=$(date -d "$PROD_DATE" '+%Y-%m-%d')
    PROD_DATE=$(date -d "$PROD_DATE 7 days" '+%Y-%m-%d')
    echo "Using product limit date : ${PROD_DATE}"
    if [[ ! -z ${PROD_DATE} ]] ; then 
        ACQ_DATE_LIMIT_OPT="-acqsdatelimit ${PROD_DATE}"    
    fi
fi 

if [ -z ${PREV_PRODUCT} ] ; then
    echo "No previous product will be used!"
    PREV_PRODUCT_OPTION=""
else
    if [ -d ${PREV_PRODUCT} ] ; then
        PREV_PRODUCT_OPTION="-prevprd ${PREV_PRODUCT}"
    else
        echo "The previous product directory provided does not exists! Exiting ..."
        exit 1
    fi
fi 

COUNTRY="${COUNTRY_AND_REGION%%_*}"
COUNTRY_REGION=""
if [ "$1" != "$COUNTRY" ] ; then

    COUNTRY_REGION="${1##*_}"
fi    

SHP_PATH=""
IN_SHP_NAME=""

WORKING_DIR_ROOT="/mnt/archive/agric_practices"
INSITU_ROOT="$WORKING_DIR_ROOT/insitu/PracticesInfos"
OUTPUTS_ROOT="${WORKING_DIR_ROOT}/Outputs/${COUNTRY_AND_REGION}/"
INPUTS_EXTRACTED_DATA_ROOT="${OUTPUTS_ROOT}/DataExtractionResults/Compact/"
OUT_DIR="$OUTPUTS_ROOT/TimeSeriesAnalysisResults/"

FILTER_IDS_FILE="${OUT_DIR}/Sen4CAP_L4C_${YEAR}_FilterIDs.csv"

CC_INPUT_PRACTICES_TABLE="${INSITU_ROOT}/Sen4CAP_L4C_Catch_${COUNTRY_AND_REGION}_${YEAR}.csv"
NFC_INPUT_PRACTICES_TABLE="${INSITU_ROOT}/Sen4CAP_L4C_NFC_${COUNTRY_AND_REGION}_${YEAR}.csv"
FL_INPUT_PRACTICES_TABLE="${INSITU_ROOT}/Sen4CAP_L4C_Fallow_${COUNTRY_AND_REGION}_${YEAR}.csv"
NA_INPUT_PRACTICES_TABLE="${INSITU_ROOT}/Sen4CAP_L4C_NA_${COUNTRY_AND_REGION}_${YEAR}.csv"

IN_AMP_PATH="${INPUTS_EXTRACTED_DATA_ROOT}/${COUNTRY_AND_REGION}_${YEAR}_AMP_Extracted_Data.csv"
IN_COHE_PATH="${INPUTS_EXTRACTED_DATA_ROOT}/${COUNTRY_AND_REGION}_${YEAR}_COHE_Extracted_Data.csv"
IN_NDVI_PATH="${INPUTS_EXTRACTED_DATA_ROOT}/${COUNTRY_AND_REGION}_${YEAR}_NDVI_Extracted_Data.csv"

DEBUG_MODE=0
ALLOW_GAPS=1
PLOT_GRAPH=1
RES_CONT_PRD=0
MIN_ACQS=15

EXECUTE_CC=1
EXECUTE_FL=1
EXECUTE_NFC=1

S1PIX=1

OPTTHRVEGCYCLE=350 
NDVIDW=300 
NDVIUP=350 
NDVISTEP=5 
OPTTHRMIN=100 
COHTHRBASE=0.05 
COHTHRHIGH=0.15 
COHTHRABS=0.75 
AMPTHRMIN=0.1 
CATCHMAIN="" 
CATCHPERIOD=56 
CATCHPERIODSTART="" 
CATCHCROPISMAIN=""
CATCHPROPORTION=""
EFANDVITHR=325 
EFANDVIUP=400 
EFANDVIDW=300 
EFACOHCHANGE=0.2 
EFACOHVALUE=0.7 
EFANDVIMIN="-10000" 
EFAAMPTHR="-10000" 
STDDEVINAMPTHR=1 
OPTTHRBUFDEN=4 
AMPTHRBREAKDEN=6
AMPTHRVALDEN=2
FLMARKSTARTDATE=""
FLMARKSTENDDATE=""

# Common values for NA to avoid setting them for each country
NA_EFANDVITHR="-10000" 
NA_EFANDVIUP="-10000" 
NA_EFANDVIDW="-10000" 
NA_EFACOHCHANGE="-10000" 
NA_EFACOHVALUE="-10000"

case "$COUNTRY" in
    NLD)
        CC_CATCHMAIN="CatchCrop_3" 
        CC_CATCHPERIODSTART="${YEAR}-07-15"
        
        CC_NDVIUP=500
        CC_AMPTHRMIN=0.2
        CC_COHTHRBASE=0.1
        CC_COHTHRABS=0.7
        CC_EFAAMPTHR=0.03
        CC_AMPTHRBREAKDEN=3
        CC_AMPTHRVALDEN=3
        
        NA_NDVIUP=500
        NA_AMPTHRMIN=0.2
        NA_COHTHRBASE=0.1
        NA_COHTHRABS=0.7
        NA_AMPTHRBREAKDEN=3
        NA_AMPTHRVALDEN=3
        
        # we do not have FL and NFC
        EXECUTE_FL=""
        EXECUTE_NFC=""
        ;;
    CZE)
        # TODO: See if this remains like this
#        S1PIX=8
        
        CC_NDVIUP=500
        CC_AMPTHRMIN=0.2
        CC_COHTHRBASE=0.1
        CC_COHTHRABS=0.7
        CC_EFAAMPTHR=0.03
        CC_CATCHMAIN="-" 
        CC_AMPTHRBREAKDEN=3
        CC_AMPTHRVALDEN=3
        
        FL_OPTTHRVEGCYCLE=500
        FL_COHTHRBASE=0.1
        FL_COHTHRABS=0.7
        FL_AMPTHRMIN=0.2
        FL_EFANDVITHR=400
        FL_EFANDVIUP=600
        FL_EFANDVIDW=600
        FL_EFACOHCHANGE="-10000"
        FL_EFACOHVALUE="-10000" 
        FL_STDDEVINAMPTHR=0 
        FL_OPTTHRBUFDEN=6 
        FL_AMPTHRBREAKDEN=3
        FL_AMPTHRVALDEN=3
        FL_FLMARKSTARTDATE="${YEAR}-06-01" 
        FL_FLMARKSTENDDATE="${YEAR}-08-31"

        NFC_OPTTHRVEGCYCLE=500
        NFC_COHTHRBASE=0.1
        NFC_COHTHRHIGH=0.1
        NFC_COHTHRABS=0.7
        NFC_AMPTHRMIN=0.2
        NFC_EFANDVITHR=350
        NFC_EFANDVIUP="-10000" 
        NFC_EFANDVIDW="-10000"
        NFC_EFACOHCHANGE="-10000" 
        NFC_EFACOHVALUE="-10000"
        NFC_OPTTHRBUFDEN=8
        NFC_AMPTHRBREAKDEN=3
        NFC_AMPTHRVALDEN=3

        NA_NDVIUP=500
        NA_AMPTHRMIN=0.2
        NA_COHTHRBASE=0.1    
        NA_COHTHRABS=0.7 
        NA_AMPTHRBREAKDEN=3
        NA_AMPTHRVALDEN=3
        ;;
    LTU)
        CC_NDVIUP=500 
        CC_COHTHRBASE=0.1
        CC_COHTHRABS=0.7
        CC_AMPTHRMIN=0.2
        CC_CATCHMAIN="IS" 
        CC_CATCHPERIOD="-10000" 
        CC_EFANDVIUP=350
        CC_EFAAMPTHR=0.03 
        CC_OPTTHRBUFDEN=4
        CC_AMPTHRBREAKDEN=3
        CC_AMPTHRVALDEN=3
        
        FL_OPTTHRVEGCYCLE=400 
        FL_NDVIUP=450 
        FL_COHTHRBASE=0.1 
        FL_COHTHRHIGH=0.2
        FL_COHTHRABS=0.7 
        FL_AMPTHRMIN=0.2
        FL_EFANDVITHR=400 
        FL_EFANDVIUP="-10000" 
        FL_EFANDVIDW="-10000" 
        FL_OPTTHRBUFDEN=6
        FL_AMPTHRBREAKDEN=3
        FL_AMPTHRVALDEN=3

        NFC_OPTTHRVEGCYCLE=400
        NFC_NDVIUP=450  
        NFC_COHTHRBASE=0.1
        NFC_COHTHRABS=0.7 
        NFC_AMPTHRMIN=0.2
        NFC_EFANDVITHR=400 
        NFC_EFANDVIUP="-10000" 
        NFC_EFANDVIDW="-10000" 
        NFC_EFACOHCHANGE="-10000" 
        NFC_EFACOHVALUE="-10000"
        NFC_AMPTHRBREAKDEN=3
        NFC_AMPTHRVALDEN=3
        
        NA_NDVIUP=500 
        NA_AMPTHRMIN=0.2
        NA_COHTHRBASE=0.1    
        NA_COHTHRABS=0.7 
        NA_AMPTHRBREAKDEN=3
        NA_AMPTHRVALDEN=3
        ;;
    ESP)
        EXECUTE_CC=""
        
        FL_OPTTHRVEGCYCLE=500 
        FL_NDVIDW=150
        FL_NDVIUP=400 
        FL_COHTHRBASE=0.1 
        FL_COHTHRABS=0.6
        FL_AMPTHRMIN=0.2
        FL_EFANDVITHR=400 
        FL_EFANDVIUP="-10000" 
        FL_EFANDVIDW="-10000" 
        FL_EFACOHCHANGE="-10000"
        FL_EFACOHVALUE="-10000"
        FL_STDDEVINAMPTHR=0
        FL_OPTTHRBUFDEN=6
        FL_AMPTHRBREAKDEN=3
        FL_AMPTHRVALDEN=2

        NFC_OPTTHRVEGCYCLE=325
        NFC_NDVIDW=150
        NFC_NDVIUP=450  
        NFC_COHTHRBASE=0.1
        NFC_COHTHRABS=0.6
        NFC_AMPTHRMIN=0.2
        NFC_EFANDVITHR=325 
        NFC_EFANDVIUP="-10000" 
        NFC_EFANDVIDW="-10000" 
        NFC_EFACOHCHANGE="-10000" 
        NFC_EFACOHVALUE="-10000"
        NFC_STDDEVINAMPTHR=0
        NFC_OPTTHRBUFDEN=8
        NFC_AMPTHRBREAKDEN=3
        NFC_AMPTHRVALDEN=3
        
        NA_OPTTHRVEGCYCLE=325
        NA_NDVIDW=150
        NA_NDVIUP=450 
        NA_COHTHRBASE=0.1    
        NA_COHTHRABS=0.6 
        NA_AMPTHRMIN=0.2
        NA_OPTTHRBUFDEN=8
        NA_AMPTHRBREAKDEN=3
        NA_AMPTHRVALDEN=3
        ;;
    ITA)
        EXECUTE_CC=""
        
        FL_OPTTHRVEGCYCLE=500 
        FL_NDVIDW=300
        FL_NDVIUP=400 
        FL_OPTTHRMIN=10
        FL_COHTHRBASE=0.1 
        FL_COHTHRABS=0.6
        FL_AMPTHRMIN=0.2
        FL_EFANDVITHR=400 
        FL_EFANDVIUP="-10000" 
        FL_EFANDVIDW="-10000" 
        FL_EFACOHCHANGE="-10000"
        FL_EFACOHVALUE="-10000"
        FL_STDDEVINAMPTHR=0
        FL_OPTTHRBUFDEN=6
        FL_AMPTHRBREAKDEN=3
        FL_AMPTHRVALDEN=1
        FL_FLMARKSTARTDATE="${YEAR}-03-31" 

        NFC_OPTTHRVEGCYCLE=350
        NFC_NDVIDW=300
        NFC_NDVIUP=450  
        NFC_OPTTHRMIN=10
        NFC_COHTHRBASE=0.1
        NFC_COHTHRABS=0.6
        NFC_AMPTHRMIN=0.2
        NFC_EFANDVITHR=350
        NFC_EFANDVIUP="-10000" 
        NFC_EFANDVIDW="-10000" 
        NFC_EFACOHCHANGE="-10000" 
        NFC_EFACOHVALUE="-10000"
        NFC_STDDEVINAMPTHR=1
        NFC_OPTTHRBUFDEN=6
        NFC_AMPTHRBREAKDEN=3
        NFC_AMPTHRVALDEN=3
        
        NA_OPTTHRVEGCYCLE=350
        NA_NDVIDW=300
        NA_NDVIUP=450 
        NA_OPTTHRMIN=10
        NA_COHTHRBASE=0.1    
        NA_COHTHRABS=0.6 
        NA_AMPTHRMIN=0.2
        NA_OPTTHRBUFDEN=6
        NA_AMPTHRBREAKDEN=3
        NA_AMPTHRVALDEN=3
        ;;
    ROU)
        EXECUTE_FL=""
        CC_OPTTHRVEGCYCLE=400
        CC_NDVIUP=400 
        CC_COHTHRBASE=0.1
        CC_COHTHRHIGH=0.1
        CC_COHTHRABS=0.6
        CC_AMPTHRMIN=0.2
        CC_CATCHMAIN="-" 
        CC_CATCHPERIOD=56
        CC_CATCHPERIODSTART="${YEAR}-08-01"
        CC_EFANDVIUP=350
        CC_EFAAMPTHR=0.03 
        CC_EFACOHVALUE=0.6
        CC_OPTTHRBUFDEN=6
        CC_AMPTHRBREAKDEN=4
        CC_AMPTHRVALDEN=3
        
        NFC_OPTTHRVEGCYCLE=400
        NFC_NDVIUP=400  
        NFC_COHTHRBASE=0.1
        NFC_COHTHRABS=0.6
        NFC_AMPTHRMIN=0.2
        NFC_EFANDVITHR=350 
        NFC_EFANDVIUP="-10000" 
        NFC_EFANDVIDW="-10000" 
        NFC_EFACOHCHANGE="-10000" 
        NFC_EFACOHVALUE="-10000"
        NFC_OPTTHRBUFDEN=6
        NFC_AMPTHRBREAKDEN=3
        NFC_AMPTHRVALDEN=3
        
        NA_OPTTHRVEGCYCLE=400
        NA_NDVIUP=400 
        NA_COHTHRBASE=0.1   
        NA_COHTHRHIGH=0.1        
        NA_COHTHRABS=0.6
        NA_AMPTHRMIN=0.2
        NA_OPTTHRBUFDEN=6      
        NA_AMPTHRBREAKDEN=4
        NA_AMPTHRVALDEN=3
        ;;
    *)
        echo $"Usage: $0 {NLD|CZE|LTU|ESP|ITA|ROU}"
        exit 1
esac

SHP_PATH="$INSITU_ROOT$IN_SHP_NAME"

if [ -z "$CC_OPTTHRVEGCYCLE" ];     then CC_OPTTHRVEGCYCLE="$OPTTHRVEGCYCLE" ; fi 
if [ -z "$CC_NDVIDW" ];             then CC_NDVIDW="$NDVIDW" ; fi 
if [ -z "$CC_NDVIUP" ];             then CC_NDVIUP="$NDVIUP" ; fi 
if [ -z "$CC_NDVISTEP" ];           then CC_NDVISTEP="$NDVISTEP" ; fi 
if [ -z "$CC_OPTTHRMIN" ];          then CC_OPTTHRMIN="$OPTTHRMIN" ; fi 
if [ -z "$CC_COHTHRBASE" ];         then CC_COHTHRBASE="$COHTHRBASE" ; fi 
if [ -z "$CC_COHTHRHIGH" ];         then CC_COHTHRHIGH="$COHTHRHIGH" ; fi 
if [ -z "$CC_COHTHRABS" ];          then CC_COHTHRABS="$COHTHRABS" ; fi 
if [ -z "$CC_AMPTHRMIN" ];          then CC_AMPTHRMIN="$AMPTHRMIN" ; fi 
if [ -z "$CC_CATCHMAIN" ];          then CC_CATCHMAIN="$CATCHMAIN" ; fi 
if [ -z "$CC_CATCHPERIOD" ];        then CC_CATCHPERIOD="$CATCHPERIOD" ; fi 
if [ -z "$CC_CATCHPERIODSTART" ];   then CC_CATCHPERIODSTART="$CATCHPERIODSTART" ; fi 
if [ -z "$CC_CATCHCROPISMAIN" ];    then CC_CATCHCROPISMAIN="$CATCHCROPISMAIN" ; fi 
if [ -z "$CC_CATCHPROPORTION" ];    then CC_CATCHPROPORTION="$CATCHPROPORTION" ; fi 
if [ -z "$CC_EFANDVITHR" ];         then CC_EFANDVITHR="$EFANDVITHR" ; fi 
if [ -z "$CC_EFANDVIUP" ];          then CC_EFANDVIUP="$EFANDVIUP" ; fi 
if [ -z "$CC_EFANDVIDW" ];          then CC_EFANDVIDW="$EFANDVIDW" ; fi 
if [ -z "$CC_EFACOHCHANGE" ];       then CC_EFACOHCHANGE="$EFACOHCHANGE" ; fi 
if [ -z "$CC_EFACOHVALUE" ];        then CC_EFACOHVALUE="$EFACOHVALUE" ; fi 
if [ -z "$CC_EFANDVIMIN" ];         then CC_EFANDVIMIN="$EFANDVIMIN" ; fi 
if [ -z "$CC_EFAAMPTHR" ];          then CC_EFAAMPTHR="$EFAAMPTHR" ; fi 
if [ -z "$CC_STDDEVINAMPTHR" ];     then CC_STDDEVINAMPTHR="$STDDEVINAMPTHR" ; fi 
if [ -z "$CC_OPTTHRBUFDEN" ];       then CC_OPTTHRBUFDEN="$OPTTHRBUFDEN" ; fi 
if [ -z "$CC_AMPTHRBREAKDEN" ];     then CC_AMPTHRBREAKDEN="$AMPTHRBREAKDEN" ; fi 
if [ -z "$CC_AMPTHRVALDEN" ];     then CC_AMPTHRVALDEN="$AMPTHRVALDEN" ; fi 

if [ -n "$CC_OPTTHRVEGCYCLE" ];     then CC_OPTTHRVEGCYCLE="-optthrvegcycle \"$CC_OPTTHRVEGCYCLE\" " ; fi
if [ -n "$CC_NDVIDW" ];             then CC_NDVIDW="-ndvidw \"$CC_NDVIDW\" " ; fi
if [ -n "$CC_NDVIUP" ];             then CC_NDVIUP="-ndviup \"$CC_NDVIUP\" " ; fi
if [ -n "$CC_NDVISTEP" ];           then CC_NDVISTEP="-ndvistep \"$CC_NDVISTEP\" " ; fi
if [ -n "$CC_OPTTHRMIN" ];          then CC_OPTTHRMIN="-optthrmin \"$CC_OPTTHRMIN\" " ; fi
if [ -n "$CC_COHTHRBASE" ];         then CC_COHTHRBASE="-cohthrbase \"$CC_COHTHRBASE\" " ; fi
if [ -n "$CC_COHTHRHIGH" ];         then CC_COHTHRHIGH="-cohthrhigh \"$CC_COHTHRHIGH\" " ; fi
if [ -n "$CC_COHTHRABS" ];          then CC_COHTHRABS="-cohthrabs \"$CC_COHTHRABS\" " ; fi
if [ -n "$CC_AMPTHRMIN" ];          then CC_AMPTHRMIN="-ampthrmin \"$CC_AMPTHRMIN\" " ; fi
if [ -n "$CC_CATCHMAIN" ];          then CC_CATCHMAIN="-catchmain \"$CC_CATCHMAIN\" " ; fi
if [ -n "$CC_CATCHPERIOD" ];        then CC_CATCHPERIOD="-catchperiod \"$CC_CATCHPERIOD\" " ; fi
if [ -n "$CC_CATCHPERIODSTART" ];   then CC_CATCHPERIODSTART="-catchperiodstart \"$CC_CATCHPERIODSTART\" " ; fi
if [ -n "$CC_CATCHCROPISMAIN" ];    then CC_CATCHCROPISMAIN="-catchcropismain \"$CC_CATCHCROPISMAIN\" " ; fi
if [ -n "$CC_CATCHPROPORTION" ];    then CC_CATCHPROPORTION="-catchproportion \"$CC_CATCHPROPORTION\" " ; fi
if [ -n "$CC_EFANDVITHR" ];         then CC_EFANDVITHR="-efandvithr \"$CC_EFANDVITHR\" " ; fi
if [ -n "$CC_EFANDVIUP" ];          then CC_EFANDVIUP="-efandviup \"$CC_EFANDVIUP\" " ; fi
if [ -n "$CC_EFANDVIDW" ];          then CC_EFANDVIDW="-efandvidw \"$CC_EFANDVIDW\" " ; fi
if [ -n "$CC_EFACOHCHANGE" ];       then CC_EFACOHCHANGE="-efacohchange \"$CC_EFACOHCHANGE\" " ; fi
if [ -n "$CC_EFACOHVALUE" ];        then CC_EFACOHVALUE="-efacohvalue \"$CC_EFACOHVALUE\" " ; fi
if [ -n "$CC_EFANDVIMIN" ];         then CC_EFANDVIMIN="-efandvimin \"$CC_EFANDVIMIN\" " ; fi
if [ -n "$CC_EFAAMPTHR" ];          then CC_EFAAMPTHR="-efaampthr \"$CC_EFAAMPTHR\" " ; fi
if [ -n "$CC_STDDEVINAMPTHR" ];     then CC_STDDEVINAMPTHR="-stddevinampthr \"$CC_STDDEVINAMPTHR\" " ; fi
if [ -n "$CC_OPTTHRBUFDEN" ];       then CC_OPTTHRBUFDEN="-optthrbufden \"$CC_OPTTHRBUFDEN\" " ; fi
if [ -n "$CC_AMPTHRBREAKDEN" ];     then CC_AMPTHRBREAKDEN="-ampthrbreakden \"$CC_AMPTHRBREAKDEN\" " ; fi
if [ -n "$CC_AMPTHRVALDEN" ];     then CC_AMPTHRVALDEN="-ampthrvalden \"$CC_AMPTHRVALDEN\" " ; fi

if [ -z "$NFC_OPTTHRVEGCYCLE" ];     then NFC_OPTTHRVEGCYCLE="$OPTTHRVEGCYCLE" ; fi 
if [ -z "$NFC_NDVIDW" ];             then NFC_NDVIDW="$NDVIDW" ; fi 
if [ -z "$NFC_NDVIUP" ];             then NFC_NDVIUP="$NDVIUP" ; fi 
if [ -z "$NFC_NDVISTEP" ];           then NFC_NDVISTEP="$NDVISTEP" ; fi 
if [ -z "$NFC_OPTTHRMIN" ];          then NFC_OPTTHRMIN="$OPTTHRMIN" ; fi 
if [ -z "$NFC_COHTHRBASE" ];         then NFC_COHTHRBASE="$COHTHRBASE" ; fi 
if [ -z "$NFC_COHTHRHIGH" ];         then NFC_COHTHRHIGH="$COHTHRHIGH" ; fi 
if [ -z "$NFC_COHTHRABS" ];          then NFC_COHTHRABS="$COHTHRABS" ; fi 
if [ -z "$NFC_AMPTHRMIN" ];          then NFC_AMPTHRMIN="$AMPTHRMIN" ; fi 
if [ -z "$NFC_EFANDVITHR" ];         then NFC_EFANDVITHR="$EFANDVITHR" ; fi 
if [ -z "$NFC_EFANDVIUP" ];          then NFC_EFANDVIUP="$EFANDVIUP" ; fi 
if [ -z "$NFC_EFANDVIDW" ];          then NFC_EFANDVIDW="$EFANDVIDW" ; fi 
if [ -z "$NFC_EFACOHCHANGE" ];       then NFC_EFACOHCHANGE="$EFACOHCHANGE" ; fi 
if [ -z "$NFC_EFACOHVALUE" ];        then NFC_EFACOHVALUE="$EFACOHVALUE" ; fi 
if [ -z "$NFC_EFANDVIMIN" ];         then NFC_EFANDVIMIN="$EFANDVIMIN" ; fi 
if [ -z "$NFC_EFAAMPTHR" ];          then NFC_EFAAMPTHR="$EFAAMPTHR" ; fi 
if [ -z "$NFC_STDDEVINAMPTHR" ];     then NFC_STDDEVINAMPTHR="$STDDEVINAMPTHR" ; fi 
if [ -z "$NFC_OPTTHRBUFDEN" ];       then NFC_OPTTHRBUFDEN="$OPTTHRBUFDEN" ; fi 
if [ -z "$NFC_AMPTHRBREAKDEN" ];     then NFC_AMPTHRBREAKDEN="$AMPTHRBREAKDEN" ; fi 
if [ -z "$NFC_AMPTHRVALDEN" ];     then NFC_AMPTHRVALDEN="$AMPTHRVALDEN" ; fi 


if [ -n "$NFC_OPTTHRVEGCYCLE" ];     then NFC_OPTTHRVEGCYCLE="-optthrvegcycle \"$NFC_OPTTHRVEGCYCLE\" " ; fi
if [ -n "$NFC_NDVIDW" ];             then NFC_NDVIDW="-ndvidw \"$NFC_NDVIDW\" " ; fi
if [ -n "$NFC_NDVIUP" ];             then NFC_NDVIUP="-ndviup \"$NFC_NDVIUP\" " ; fi
if [ -n "$NFC_NDVISTEP" ];           then NFC_NDVISTEP="-ndvistep \"$NFC_NDVISTEP\" " ; fi
if [ -n "$NFC_OPTTHRMIN" ];          then NFC_OPTTHRMIN="-optthrmin \"$NFC_OPTTHRMIN\" " ; fi
if [ -n "$NFC_COHTHRBASE" ];         then NFC_COHTHRBASE="-cohthrbase \"$NFC_COHTHRBASE\" " ; fi
if [ -n "$NFC_COHTHRHIGH" ];         then NFC_COHTHRHIGH="-cohthrhigh \"$NFC_COHTHRHIGH\" " ; fi
if [ -n "$NFC_COHTHRABS" ];          then NFC_COHTHRABS="-cohthrabs \"$NFC_COHTHRABS\" " ; fi
if [ -n "$NFC_AMPTHRMIN" ];          then NFC_AMPTHRMIN="-ampthrmin \"$NFC_AMPTHRMIN\" " ; fi
if [ -n "$NFC_EFANDVITHR" ];         then NFC_EFANDVITHR="-efandvithr \"$NFC_EFANDVITHR\" " ; fi
if [ -n "$NFC_EFANDVIUP" ];          then NFC_EFANDVIUP="-efandviup \"$NFC_EFANDVIUP\" " ; fi
if [ -n "$NFC_EFANDVIDW" ];          then NFC_EFANDVIDW="-efandvidw \"$NFC_EFANDVIDW\" " ; fi
if [ -n "$NFC_EFACOHCHANGE" ];       then NFC_EFACOHCHANGE="-efacohchange \"$NFC_EFACOHCHANGE\" " ; fi
if [ -n "$NFC_EFACOHVALUE" ];        then NFC_EFACOHVALUE="-efacohvalue \"$NFC_EFACOHVALUE\" " ; fi
if [ -n "$NFC_EFANDVIMIN" ];         then NFC_EFANDVIMIN="-efandvimin \"$NFC_EFANDVIMIN\" " ; fi
if [ -n "$NFC_EFAAMPTHR" ];          then NFC_EFAAMPTHR="-efaampthr \"$NFC_EFAAMPTHR\" " ; fi
if [ -n "$NFC_STDDEVINAMPTHR" ];     then NFC_STDDEVINAMPTHR="-stddevinampthr \"$NFC_STDDEVINAMPTHR\" " ; fi
if [ -n "$NFC_OPTTHRBUFDEN" ];       then NFC_OPTTHRBUFDEN="-optthrbufden \"$NFC_OPTTHRBUFDEN\" " ; fi
if [ -n "$NFC_AMPTHRBREAKDEN" ];     then NFC_AMPTHRBREAKDEN="-ampthrbreakden \"$NFC_AMPTHRBREAKDEN\" " ; fi
if [ -n "$NFC_AMPTHRVALDEN" ];     then NFC_AMPTHRVALDEN="-ampthrvalden \"$NFC_AMPTHRVALDEN\" " ; fi


if [ -z "$FL_OPTTHRVEGCYCLE" ];     then FL_OPTTHRVEGCYCLE="$OPTTHRVEGCYCLE" ; fi 
if [ -z "$FL_NDVIDW" ];             then FL_NDVIDW="$NDVIDW" ; fi 
if [ -z "$FL_NDVIUP" ];             then FL_NDVIUP="$NDVIUP" ; fi 
if [ -z "$FL_NDVISTEP" ];           then FL_NDVISTEP="$NDVISTEP" ; fi 
if [ -z "$FL_OPTTHRMIN" ];          then FL_OPTTHRMIN="$OPTTHRMIN" ; fi 
if [ -z "$FL_COHTHRBASE" ];         then FL_COHTHRBASE="$COHTHRBASE" ; fi 
if [ -z "$FL_COHTHRHIGH" ];         then FL_COHTHRHIGH="$COHTHRHIGH" ; fi 
if [ -z "$FL_COHTHRABS" ];          then FL_COHTHRABS="$COHTHRABS" ; fi 
if [ -z "$FL_AMPTHRMIN" ];          then FL_AMPTHRMIN="$AMPTHRMIN" ; fi 
if [ -z "$FL_EFANDVITHR" ];         then FL_EFANDVITHR="$EFANDVITHR" ; fi 
if [ -z "$FL_EFANDVIUP" ];          then FL_EFANDVIUP="$EFANDVIUP" ; fi 
if [ -z "$FL_EFANDVIDW" ];          then FL_EFANDVIDW="$EFANDVIDW" ; fi 
if [ -z "$FL_EFACOHCHANGE" ];       then FL_EFACOHCHANGE="$EFACOHCHANGE" ; fi 
if [ -z "$FL_EFACOHVALUE" ];        then FL_EFACOHVALUE="$EFACOHVALUE" ; fi 
if [ -z "$FL_EFANDVIMIN" ];         then FL_EFANDVIMIN="$EFANDVIMIN" ; fi 
if [ -z "$FL_EFAAMPTHR" ];          then FL_EFAAMPTHR="$EFAAMPTHR" ; fi 
if [ -z "$FL_STDDEVINAMPTHR" ];     then FL_STDDEVINAMPTHR="$STDDEVINAMPTHR" ; fi 
if [ -z "$FL_OPTTHRBUFDEN" ];       then FL_OPTTHRBUFDEN="$OPTTHRBUFDEN" ; fi 
if [ -z "$FL_AMPTHRBREAKDEN" ];     then FL_AMPTHRBREAKDEN="$AMPTHRBREAKDEN" ; fi 
if [ -z "$FL_AMPTHRVALDEN" ];     then FL_AMPTHRVALDEN="$AMPTHRVALDEN" ; fi 
if [ -z "$FL_FLMARKSTARTDATE" ];    then FL_FLMARKSTARTDATE="$FLMARKSTARTDATE" ; fi 
if [ -z "$FL_FLMARKSTENDDATE" ];    then FL_FLMARKSTENDDATE="$FLMARKSTENDDATE" ; fi 

if [ -n "$FL_OPTTHRVEGCYCLE" ];     then FL_OPTTHRVEGCYCLE="-optthrvegcycle \"$FL_OPTTHRVEGCYCLE\" " ; fi
if [ -n "$FL_NDVIDW" ];             then FL_NDVIDW="-ndvidw \"$FL_NDVIDW\" " ; fi
if [ -n "$FL_NDVIUP" ];             then FL_NDVIUP="-ndviup \"$FL_NDVIUP\" " ; fi
if [ -n "$FL_NDVISTEP" ];           then FL_NDVISTEP="-ndvistep \"$FL_NDVISTEP\" " ; fi
if [ -n "$FL_OPTTHRMIN" ];          then FL_OPTTHRMIN="-optthrmin \"$FL_OPTTHRMIN\" " ; fi
if [ -n "$FL_COHTHRBASE" ];         then FL_COHTHRBASE="-cohthrbase \"$FL_COHTHRBASE\" " ; fi
if [ -n "$FL_COHTHRHIGH" ];         then FL_COHTHRHIGH="-cohthrhigh \"$FL_COHTHRHIGH\" " ; fi
if [ -n "$FL_COHTHRABS" ];          then FL_COHTHRABS="-cohthrabs \"$FL_COHTHRABS\" " ; fi
if [ -n "$FL_AMPTHRMIN" ];          then FL_AMPTHRMIN="-ampthrmin \"$FL_AMPTHRMIN\" " ; fi
if [ -n "$FL_EFANDVITHR" ];         then FL_EFANDVITHR="-efandvithr \"$FL_EFANDVITHR\" " ; fi
if [ -n "$FL_EFANDVIUP" ];          then FL_EFANDVIUP="-efandviup \"$FL_EFANDVIUP\" " ; fi
if [ -n "$FL_EFANDVIDW" ];          then FL_EFANDVIDW="-efandvidw \"$FL_EFANDVIDW\" " ; fi
if [ -n "$FL_EFACOHCHANGE" ];       then FL_EFACOHCHANGE="-efacohchange \"$FL_EFACOHCHANGE\" " ; fi
if [ -n "$FL_EFACOHVALUE" ];        then FL_EFACOHVALUE="-efacohvalue \"$FL_EFACOHVALUE\" " ; fi
if [ -n "$FL_EFANDVIMIN" ];         then FL_EFANDVIMIN="-efandvimin \"$FL_EFANDVIMIN\" " ; fi
if [ -n "$FL_EFAAMPTHR" ];          then FL_EFAAMPTHR="-efaampthr \"$FL_EFAAMPTHR\" " ; fi
if [ -n "$FL_STDDEVINAMPTHR" ];     then FL_STDDEVINAMPTHR="-stddevinampthr \"$FL_STDDEVINAMPTHR\" " ; fi
if [ -n "$FL_OPTTHRBUFDEN" ];       then FL_OPTTHRBUFDEN="-optthrbufden \"$FL_OPTTHRBUFDEN\" " ; fi
if [ -n "$FL_AMPTHRBREAKDEN" ];     then FL_AMPTHRBREAKDEN="-ampthrbreakden \"$FL_AMPTHRBREAKDEN\" " ; fi
if [ -n "$FL_AMPTHRVALDEN" ];     then FL_AMPTHRVALDEN="-ampthrvalden \"$FL_AMPTHRVALDEN\" " ; fi
if [ -n "$FL_FLMARKSTARTDATE" ];    then FL_FLMARKSTARTDATE="-flmarkstartdate $FL_FLMARKSTARTDATE" ; fi 
if [ -n "$FL_FLMARKSTENDDATE" ];    then FL_FLMARKSTENDDATE="-flmarkstenddate $FL_FLMARKSTENDDATE" ; fi 

if [ -z "$NA_OPTTHRVEGCYCLE" ];     then NA_OPTTHRVEGCYCLE="$OPTTHRVEGCYCLE" ; fi 
if [ -z "$NA_NDVIDW" ];             then NA_NDVIDW="$NDVIDW" ; fi 
if [ -z "$NA_NDVIUP" ];             then NA_NDVIUP="$NDVIUP" ; fi 
if [ -z "$NA_NDVISTEP" ];           then NA_NDVISTEP="$NDVISTEP" ; fi 
if [ -z "$NA_OPTTHRMIN" ];          then NA_OPTTHRMIN="$OPTTHRMIN" ; fi 
if [ -z "$NA_COHTHRBASE" ];         then NA_COHTHRBASE="$COHTHRBASE" ; fi 
if [ -z "$NA_COHTHRHIGH" ];         then NA_COHTHRHIGH="$COHTHRHIGH" ; fi 
if [ -z "$NA_COHTHRABS" ];          then NA_COHTHRABS="$COHTHRABS" ; fi 
if [ -z "$NA_AMPTHRMIN" ];          then NA_AMPTHRMIN="$AMPTHRMIN" ; fi 
if [ -z "$NA_EFANDVITHR" ];         then NA_EFANDVITHR="$EFANDVITHR" ; fi 
if [ -z "$NA_EFANDVIUP" ];          then NA_EFANDVIUP="$EFANDVIUP" ; fi 
if [ -z "$NA_EFANDVIDW" ];          then NA_EFANDVIDW="$EFANDVIDW" ; fi 
if [ -z "$NA_EFACOHCHANGE" ];       then NA_EFACOHCHANGE="$EFACOHCHANGE" ; fi 
if [ -z "$NA_EFACOHVALUE" ];        then NA_EFACOHVALUE="$EFACOHVALUE" ; fi 
if [ -z "$NA_EFANDVIMIN" ];         then NA_EFANDVIMIN="$EFANDVIMIN" ; fi 
if [ -z "$NA_EFAAMPTHR" ];          then NA_EFAAMPTHR="$EFAAMPTHR" ; fi 
if [ -z "$NA_STDDEVINAMPTHR" ];     then NA_STDDEVINAMPTHR="$STDDEVINAMPTHR" ; fi 
if [ -z "$NA_OPTTHRBUFDEN" ];       then NA_OPTTHRBUFDEN="$OPTTHRBUFDEN" ; fi 
if [ -z "$NA_AMPTHRBREAKDEN" ];     then NA_AMPTHRBREAKDEN="$AMPTHRBREAKDEN" ; fi 
if [ -z "$NA_AMPTHRVALDEN" ];     then NA_AMPTHRVALDEN="$AMPTHRVALDEN" ; fi 

if [ -n "$NA_OPTTHRVEGCYCLE" ];     then NA_OPTTHRVEGCYCLE="-optthrvegcycle \"$NA_OPTTHRVEGCYCLE\" " ; fi
if [ -n "$NA_NDVIDW" ];             then NA_NDVIDW="-ndvidw \"$NA_NDVIDW\" " ; fi
if [ -n "$NA_NDVIUP" ];             then NA_NDVIUP="-ndviup \"$NA_NDVIUP\" " ; fi
if [ -n "$NA_NDVISTEP" ];           then NA_NDVISTEP="-ndvistep \"$NA_NDVISTEP\" " ; fi
if [ -n "$NA_OPTTHRMIN" ];          then NA_OPTTHRMIN="-optthrmin \"$NA_OPTTHRMIN\" " ; fi
if [ -n "$NA_COHTHRBASE" ];         then NA_COHTHRBASE="-cohthrbase \"$NA_COHTHRBASE\" " ; fi
if [ -n "$NA_COHTHRHIGH" ];         then NA_COHTHRHIGH="-cohthrhigh \"$NA_COHTHRHIGH\" " ; fi
if [ -n "$NA_COHTHRABS" ];          then NA_COHTHRABS="-cohthrabs \"$NA_COHTHRABS\" " ; fi
if [ -n "$NA_AMPTHRMIN" ];          then NA_AMPTHRMIN="-ampthrmin \"$NA_AMPTHRMIN\" " ; fi
if [ -n "$NA_EFANDVITHR" ];         then NA_EFANDVITHR="-efandvithr \"$NA_EFANDVITHR\" " ; fi
if [ -n "$NA_EFANDVIUP" ];          then NA_EFANDVIUP="-efandviup \"$NA_EFANDVIUP\" " ; fi
if [ -n "$NA_EFANDVIDW" ];          then NA_EFANDVIDW="-efandvidw \"$NA_EFANDVIDW\" " ; fi
if [ -n "$NA_EFACOHCHANGE" ];       then NA_EFACOHCHANGE="-efacohchange \"$NA_EFACOHCHANGE\" " ; fi
if [ -n "$NA_EFACOHVALUE" ];        then NA_EFACOHVALUE="-efacohvalue \"$NA_EFACOHVALUE\" " ; fi
if [ -n "$NA_EFANDVIMIN" ];         then NA_EFANDVIMIN="-efandvimin \"$NA_EFANDVIMIN\" " ; fi
if [ -n "$NA_EFAAMPTHR" ];          then NA_EFAAMPTHR="-efaampthr \"$NA_EFAAMPTHR\" " ; fi
if [ -n "$NA_STDDEVINAMPTHR" ];     then NA_STDDEVINAMPTHR="-stddevinampthr \"$NA_STDDEVINAMPTHR\" " ; fi
if [ -n "$NA_OPTTHRBUFDEN" ];       then NA_OPTTHRBUFDEN="-optthrbufden \"$NA_OPTTHRBUFDEN\" " ; fi
if [ -n "$NA_AMPTHRBREAKDEN" ];     then NA_AMPTHRBREAKDEN="-ampthrbreakden \"$NA_AMPTHRBREAKDEN\" " ; fi
if [ -n "$NA_AMPTHRVALDEN" ];     then NA_AMPTHRVALDEN="-ampthrvalden \"$NA_AMPTHRVALDEN\" " ; fi

mkdir -p "$OUT_DIR"

# Execute CC
if [ ! -z "$EXECUTE_CC" ] ; then
    CMD="otbcli TimeSeriesAnalysis sen2agri-processors-build -intype csv -debug $DEBUG_MODE -allowgaps $ALLOW_GAPS -plotgraph $PLOT_GRAPH -rescontprd $RES_CONT_PRD -minacqs $MIN_ACQS ${PREV_PRODUCT_OPTION} ${ACQ_DATE_LIMIT_OPT} -country $COUNTRY -practice \"CatchCrop\" -year $YEAR $CC_OPTTHRVEGCYCLE $CC_NDVIDW $CC_NDVIUP $CC_NDVISTEP $CC_OPTTHRMIN $CC_COHTHRBASE $CC_COHTHRHIGH $CC_COHTHRABS $CC_AMPTHRMIN $CC_CATCHMAIN $CC_CATCHCROPISMAIN $CC_CATCHPERIOD $CC_CATCHPROPORTION $CC_CATCHPERIODSTART $CC_EFANDVITHR $CC_EFANDVIUP $CC_EFANDVIDW $CC_EFACOHCHANGE $CC_EFACOHVALUE $CC_EFANDVIMIN $CC_EFAAMPTHR $CC_STDDEVINAMPTHR $CC_OPTTHRBUFDEN $CC_AMPTHRBREAKDEN $CC_AMPTHRVALDEN -s1pixthr ${S1PIX} -harvestshp $CC_INPUT_PRACTICES_TABLE -diramp ${IN_AMP_PATH} -dircohe ${IN_COHE_PATH} -dirndvi ${IN_NDVI_PATH} -outdir ${OUT_DIR}"
    echo "Executing ${CMD}"
    
    #Execute the command
    eval $CMD
fi 

if [ ! -z "$EXECUTE_FL" ] ; then
    CMD="otbcli TimeSeriesAnalysis sen2agri-processors-build/ -intype csv -debug $DEBUG_MODE -allowgaps $ALLOW_GAPS -plotgraph $PLOT_GRAPH -rescontprd $RES_CONT_PRD -minacqs $MIN_ACQS ${PREV_PRODUCT_OPTION} ${ACQ_DATE_LIMIT_OPT} -country $COUNTRY -practice \"Fallow\" -year $YEAR $FL_OPTTHRVEGCYCLE $FL_NDVIDW $FL_NDVIUP $FL_NDVISTEP $FL_OPTTHRMIN $FL_COHTHRBASE $FL_COHTHRHIGH $FL_COHTHRABS $FL_AMPTHRMIN $FL_EFANDVITHR $FL_EFANDVIUP $FL_EFANDVIDW $FL_EFACOHCHANGE $FL_EFACOHVALUE $FL_EFANDVIMIN $FL_EFAAMPTHR $FL_STDDEVINAMPTHR $FL_OPTTHRBUFDEN $FL_AMPTHRBREAKDEN $FL_AMPTHRVALDEN $FL_FLMARKSTARTDATE $FL_FLMARKSTENDDATE -s1pixthr ${S1PIX} -harvestshp $FL_INPUT_PRACTICES_TABLE -diramp ${IN_AMP_PATH} -dircohe ${IN_COHE_PATH} -dirndvi ${IN_NDVI_PATH} -outdir ${OUT_DIR}"
    
    echo "Executing ${CMD}"
    
    #Execute the command
    eval $CMD    
fi

if [ ! -z "$EXECUTE_NFC" ] ; then
    CMD="otbcli TimeSeriesAnalysis sen2agri-processors-build/ -intype csv -debug $DEBUG_MODE -allowgaps $ALLOW_GAPS -plotgraph $PLOT_GRAPH -rescontprd $RES_CONT_PRD -minacqs $MIN_ACQS ${PREV_PRODUCT_OPTION} ${ACQ_DATE_LIMIT_OPT} -country $COUNTRY -practice \"NFC\" -year $YEAR $NFC_OPTTHRVEGCYCLE $NFC_NDVIDW $NFC_NDVIUP $NFC_NDVISTEP $NFC_OPTTHRMIN $NFC_COHTHRBASE $NFC_COHTHRHIGH $NFC_COHTHRABS $NFC_AMPTHRMIN $NFC_EFANDVITHR $NFC_EFANDVIUP $NFC_EFANDVIDW $NFC_EFACOHCHANGE $NFC_EFACOHVALUE $NFC_EFANDVIMIN $NFC_EFAAMPTHR $NFC_STDDEVINAMPTHR $NFC_OPTTHRBUFDEN $NFC_AMPTHRBREAKDEN $NFC_AMPTHRVALDEN -s1pixthr ${S1PIX} -harvestshp $NFC_INPUT_PRACTICES_TABLE -diramp ${IN_AMP_PATH} -dircohe ${IN_COHE_PATH} -dirndvi ${IN_NDVI_PATH} -outdir ${OUT_DIR}"
    
    echo "Executing ${CMD}"
    
    #Execute the command
    eval $CMD    
fi


# Executing for all parcels not EFA monitored
CMD="otbcli TimeSeriesAnalysis sen2agri-processors-build -intype csv -debug $DEBUG_MODE -allowgaps $ALLOW_GAPS -plotgraph $PLOT_GRAPH -rescontprd $RES_CONT_PRD -minacqs $MIN_ACQS ${PREV_PRODUCT_OPTION} ${ACQ_DATE_LIMIT_OPT} -country $COUNTRY -year $YEAR $NA_OPTTHRVEGCYCLE $NA_NDVIDW $NA_NDVIUP $NA_NDVISTEP $NA_OPTTHRMIN $NA_COHTHRBASE $NA_COHTHRHIGH $NA_COHTHRABS $NA_AMPTHRMIN $NA_EFANDVITHR $NA_EFANDVIUP $NA_EFANDVIDW $NA_EFACOHCHANGE $NA_EFACOHVALUE $NA_EFANDVIMIN $NA_EFAAMPTHR $NA_STDDEVINAMPTHR $NA_OPTTHRBUFDEN $NA_AMPTHRBREAKDEN $NA_AMPTHRVALDEN -s1pixthr ${S1PIX} -harvestshp $NA_INPUT_PRACTICES_TABLE -diramp ${IN_AMP_PATH} -dircohe ${IN_COHE_PATH} -dirndvi ${IN_NDVI_PATH} -outdir ${OUT_DIR}"

echo "Executing ${CMD}"

#Execute the command
eval $CMD    
