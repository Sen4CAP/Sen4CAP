#ifndef CommonDefs_h
#define CommonDefs_h

#define NOT_AVAILABLE               -10000
#define NR                          -10001
#define NOT_AVAILABLE_1             -10002

#define NA_STR                      "NA"
#define NR_STR                      "NR"
#define NA1_STR                     "NA1"


// TODO: To remove the CR_CAT_VAL and all its references. LC should be used instead
#define CR_CAT_VAL                      "CR_CAT"
#define LC_VAL                          "LC"

// The unique sequencial ID as it appears in the database or in the shapefile
#define SEQ_UNIQUE_ID                       "NewID"
#define ORIG_UNIQUE_ID                      "ori_id"
#define ORIG_CROP                           "ori_crop"

#define CATCH_CROP_VAL                  "CatchCrop"
#define FALLOW_LAND_VAL                 "Fallow"
#define NITROGEN_FIXING_CROP_VAL        "NFC"

#define CATCH_CROP_VAL_ID                  1
#define FALLOW_LAND_VAL_ID                 2
#define NITROGEN_FIXING_CROP_VAL_ID        3

#define SEC_IN_DAY                   86400          // seconds in day = 24 * 3600

// Optical L3B product regex
#define L3B_REGEX          R"(S2AGRI_L3B_S(NDVI|LAI|FAPAR|FCOVER)(?:MONO)?_A(\d{8})T.*\.TIF)"
// 2017 naming format for coherence and amplitude
#define S1_REGEX_OLD        R"((\d{8})(-(\d{8}))?_.*(cohe|amp).*_(\d{3})_(VH|VV)_.*\.tiff)"
// 2018 naming format for coherence and amplitude
#define S1_REGEX        R"(SEN4CAP_L2A_.*_V(\d{8})T\d{6}_(\d{8})T\d{6}_(VH|VV)_(\d{3})_(?:.+)?(AMP|COHE)\.tif)"

#define L3B_REGEX_TYPE_IDX          1
#define L3B_REGEX_DATE_IDX          2

#define S1_REGEX_DATE_IDX         1           // this is the same for 2017 and 2018 formats
#define S1_REGEX_DATE2_IDX        2           // 2018
#define S1_REGEX_POLARISATION_IDX 3           // 2018
#define S1_REGEX_ORBIT_IDX        4           // 2018
#define S1_REGEX_TYPE_IDX         5           // 2018

#define S1_REGEX_DATE2_OLD_IDX    3           // this is different for 2017
#define S1_REGEX_TYPE_OLD_IDX     4           // this is different for 2017
#define S1_REGEX_ORBIT_OLD_IDX    5           // this is different for 2017
#define S1_REGEX_POLAR_OLD_IDX    6           // this is different for 2017

#define NDVI_FT         "NDVI"
#define LAI_FT          "LAI"
#define FAPAR_FT        "FAPAR"
#define FCOVER_FT       "FCOVER"
#define AMP_FT          "AMP"
#define COHE_FT         "COHE"


#endif
