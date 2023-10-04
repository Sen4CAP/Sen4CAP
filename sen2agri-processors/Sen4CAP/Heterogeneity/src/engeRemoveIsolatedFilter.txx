#include "engeRemoveIsolatedFilter.h"
#include "itkProgressReporter.h"
#include "itkConstNeighborhoodIterator.h"
#include "itkImageRegionIterator.h"
#include <queue>
#include <math.h>
#include <map>
#include <algorithm>

template <class TI, class TO>
void engeRemoveIsolatedFilter<TI, TO>::GenerateInputRequestedRegion() {
  Superclass::GenerateInputRequestedRegion();

  InputImagePointerType  inputPtr = const_cast<TI *>(this->GetInput(0));
  OutputImagePointerType outputPtr = this->GetOutput();
  if (!inputPtr || !outputPtr)
    return;

  // input region = output + a pad area
  InputImageRegionType inputRequestedRegion = inputPtr->GetRequestedRegion();
  InputImageSizeType maxRad;
  maxRad[0] = m_radius;
  maxRad[1] = m_radius;
  inputRequestedRegion.PadByRadius(maxRad);

  // crop the input requested region at the input's largest possible region
  if (inputRequestedRegion.Crop(inputPtr->GetLargestPossibleRegion())) {
    inputPtr->SetRequestedRegion(inputRequestedRegion);
  } else {
    // Couldn't crop the region (requested region is outside the largest possible region).
    inputPtr->SetRequestedRegion(inputRequestedRegion);
    itk::InvalidRequestedRegionError e(__FILE__, __LINE__);
    std::ostringstream msg;
    msg << this->GetNameOfClass()
        << "::GenerateInputRequestedRegion()";
    e.SetLocation(msg.str().c_str());
    e.SetDescription("Requested region is (at least partially) outside the largest possible region.");
    e.SetDataObject(inputPtr);
    throw e;
  }
}



template<class TI, class TO>
void engeRemoveIsolatedFilter<TI, TO>::ThreadedGenerateData(const OutputImageRegionType& outputRegionForThread, itk::ThreadIdType threadId) {

  itk::ImageRegionIterator<TO> oIt(this->GetOutput(), outputRegionForThread);

  InputImagePointerType inputPtr = const_cast<TI *>(this->GetInput(0));
  typename itk::ConstNeighborhoodIterator<TI>::RadiusType radius;
  radius[0] = m_radius; radius[1] = m_radius;
  InputImageRegionType inputRegionForThread;
  this->CallCopyOutputRegionToInputRegion(inputRegionForThread, outputRegionForThread);
  itk::ConstNeighborhoodIterator<TI> iIt(radius, inputPtr, inputRegionForThread);

  //itk::ProgressReporter progress(this, threadId, outputRegionForThread.GetNumberOfPixels());

  itk::Offset<2> offset;
  typename TI::InternalPixelType pixelI, pixelCenter;
  typename TO::InternalPixelType pixelO;
  typename std::map<typename TO::InternalPixelType, unsigned int> count;
  typename std::map<typename TO::InternalPixelType, unsigned int>::const_iterator cIt;


  // Loop on the pixels
  for (iIt.GoToBegin(), oIt.GoToBegin(); !oIt.IsAtEnd(); ++oIt, ++iIt) {

    offset.Fill(0);
    pixelCenter = iIt.GetPixel(offset);
    pixelO = pixelCenter;
    
    // if classes list given, central pixel must be in list or kept intact
    if(!m_classes.empty() && std::find(m_classes.begin(), m_classes.end(), pixelCenter) == m_classes.end()) {
      oIt.Set( pixelO );
      continue;
    }

    count.clear();
    for(offset[0] = -m_radius; offset[0] <= m_radius; offset[0]++) {
      for(offset[1] = -m_radius; offset[1] <= m_radius; offset[1]++) {
        pixelI = iIt.GetPixel(offset);
        if( count.find( pixelI ) == count.end() )
          count[pixelI] = 0;
        count[pixelI]++;
      }
    }
    count.erase(m_NaN);
    // if pixelCenter is a nan (removed just before), or if count under or equal the minimum
    if( count.find( pixelCenter ) == count.end() || count[pixelCenter] <= m_minNeighbors ) {
      unsigned int max = 0;
      for(cIt = count.begin(); cIt != count.end(); ++cIt) {
        if(cIt->second > max) {
          max = cIt->second;
          pixelO = cIt->first;
        }
      }
      if(max == 0) { // nothing found in neightbor
        pixelO = m_NaN;
      }
    }

    // store output
    oIt.Set( pixelO );
  }

}

