#ifndef METADATAHELPERDEFS_H
#define METADATAHELPERDEFS_H

#include <string>
#include <vector>

#define UNUSED(expr)                                                                               \
    do {                                                                                           \
        (void)(expr);                                                                              \
    } while (0)

#define LANDSAT_MISSION_STR "LANDSAT"
#define SENTINEL_MISSION_STR "SENTINEL"
#define SPOT_MISSION_STR "SPOT"
#define SPOT4_MISSION_STR "SPOT4"
#define SPOT5_MISSION_STR "SPOT5"

typedef struct {
    double zenith;
    double azimuth;
} MeanAngles_Type;

struct MetadataHelperAngleList {
    std::string ColumnUnit;
    std::string ColumnStep;
    std::string RowUnit;
    std::string RowStep;
    std::vector<std::vector<double>> Values;
};

struct MetadataHelperAngles {
    MetadataHelperAngleList Zenith;
    MetadataHelperAngleList Azimuth;
};

struct MetadataHelperViewingAnglesGrid {
    std::string BandId;
    std::string DetectorId;
    MetadataHelperAngles Angles;
};

typedef enum {
    MSK_CLOUD = 1,
    MSK_SNOW = 2,
    MSK_WATER = 4,
    MSK_SAT = 8,
    MSK_VALID = 16,
    ALL = 0x1F
} MasksFlagType;


#endif // METADATAHELPERDEFS_H
