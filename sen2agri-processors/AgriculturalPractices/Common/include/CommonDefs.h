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

enum class Satellite
{
    Invalid = 0,
    Sentinel2 = 1,
    Landsat8 = 2,
    Sentinel1 = 3,
};

#endif
