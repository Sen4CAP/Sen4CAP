#ifndef MAJAMASKHANDLERFUNCTOR_H
#define MAJAMASKHANDLERFUNCTOR_H

#include "MetadataHelperDefs.h"

template< class TInput, class TOutput>
class MAJAMaskHandlerFunctor
{
public:
    MAJAMaskHandlerFunctor(){}
    void Initialize(MasksFlagType nMaskFlags, bool binarizeResult) { m_MaskFlags = nMaskFlags; m_bBinarizeResult = binarizeResult;}
    MAJAMaskHandlerFunctor& operator =(const MAJAMaskHandlerFunctor& copy) {
        m_MaskFlags=copy.m_MaskFlags;
        m_bBinarizeResult = copy.m_bBinarizeResult;
        return *this;
    }
    bool operator!=( const MAJAMaskHandlerFunctor & a) const { return (this->m_MaskFlags != a.m_MaskFlags) || (this->m_bBinarizeResult != a.m_bBinarizeResult) ;}
    bool operator==( const MAJAMaskHandlerFunctor & a ) const { return !(*this != a); }

    TOutput operator()( const TInput & B) {
        TOutput res = GetSingleBandMask(B);
        if (m_bBinarizeResult) {
            // return 0 if LAND and 1 otherwise
            return (res != IMG_FLG_LAND);
        }
        return res;
    }

    // The expected order in the array would be : Cloud/Water/Snow, Saturation, Valid
    TOutput operator()( const std::vector< TInput > & B) {
        TOutput res = computeOutput(B);
        if (m_bBinarizeResult) {
            // return 0 if LAND and 1 otherwise
            return (res != IMG_FLG_LAND);
        }
        return res;
    }

    bool IsCloud(TInput val) {
        return (((val & 0x02) != 0) || ((val & 0x08) != 0));
    }
    bool IsNoData(TInput val) {
        return val != 0;
    }
    bool IsSaturation(TInput val) {
        return val != 0;
    }
    bool IsWater(TInput val) {
        return ((val & 0x01) != 0);
    }
    bool IsSnow(TInput val) {
        return ((val & 0x04) != 0);
    }

    TOutput GetSingleBandMask(const TInput & B) {
        if(((m_MaskFlags & MSK_VALID) != 0) && IsNoData(B)) return IMG_FLG_NO_DATA;
        // check bit 2 of MG2 (cloud_mask_all_cloud, result of a “logical OR” for all the cloud masks) and
        // and bit 4 of MG2 (logical OR between CM7 and CM8): shadow masks of clouds)
        if(((m_MaskFlags & MSK_CLOUD) != 0) && IsCloud(B)) return IMG_FLG_CLOUD;
        // check MG2 mask bit 1
        if(((m_MaskFlags & MSK_WATER) != 0) && IsWater(B)) return IMG_FLG_WATER;
        // check MG2 mask bit 3
        if(((m_MaskFlags & MSK_SNOW) != 0) && IsSnow(B)) return IMG_FLG_SNOW;
        // saturation - here is not quite correct as we have the saturation distinct for each band but we do
        // not have an API for making the distinction so we  consider it globally
        if(((m_MaskFlags & MSK_SAT) != 0) && IsSaturation(B)) return IMG_FLG_SATURATION;

        // default
        return IMG_FLG_LAND;
    }

    TOutput computeOutput( const std::vector< TInput > & B) {
        // The order is  (MSK_SNOW/MSK_WATTER/MSK_CLOUD), MSK_SAT, MSK_VALID
        switch (B.size())
        {
        case 1:
            return GetSingleBandMask(B[0]);
        case 2:
            if((m_MaskFlags & MSK_VALID) != 0) {
                // EDG[1] + (CLD/WAT/SNOW  OR SAT)[0]
                if(IsNoData(B[1])) return IMG_FLG_NO_DATA;

                // EDG[1] + SAT[0]
                if((m_MaskFlags & MSK_SAT) != 0) {
                    if(IsSaturation(B[0]) != 0) return IMG_FLG_SATURATION;
                } else {
                    //EDG[1] + CLD/WAT/SNOW [0]
                    if(((m_MaskFlags & MSK_CLOUD) != 0) && IsCloud(B[0])) return IMG_FLG_CLOUD;
                    if(((m_MaskFlags & MSK_WATER) != 0) && IsWater(B[0])) return IMG_FLG_WATER;
                    if(((m_MaskFlags & MSK_SNOW) != 0) && IsSnow(B[0])) return IMG_FLG_SNOW;
                }
            } else {
                // CLD/WAT/SNOW[0] + SAT[1]
                if(((m_MaskFlags & MSK_CLOUD) != 0) && IsCloud(B[0])) return IMG_FLG_CLOUD;
                if(((m_MaskFlags & MSK_WATER) != 0) && IsWater(B[0])) return IMG_FLG_WATER;
                if(((m_MaskFlags & MSK_SNOW) != 0) && IsSnow(B[0])) return IMG_FLG_SNOW;
                if(IsSaturation(B[1]) != 0) return IMG_FLG_SATURATION;
            }
            break;
        case 3:
            if(IsNoData(B[2])) return IMG_FLG_NO_DATA;
            if(((m_MaskFlags & MSK_CLOUD) != 0) && IsCloud(B[0])) return IMG_FLG_CLOUD;
            if(((m_MaskFlags & MSK_WATER) != 0) && IsWater(B[0])) return IMG_FLG_WATER;
            if(((m_MaskFlags & MSK_SNOW) != 0) && IsSnow(B[0])) return IMG_FLG_SNOW;
            if(IsSaturation(B[1]) != 0) return IMG_FLG_SATURATION;
            break;
        }
        return IMG_FLG_LAND;
    }

private:
    MasksFlagType m_MaskFlags;
    bool m_bBinarizeResult;
};

#endif  // MAJAMASKHANDLERFUNCTOR_H
