#include "engeLocalClassConnectivityIndexFilter.h"
#include "itkProgressReporter.h"
#include "engeSimpleNeighborhoodIterator.h"
#include "itkImageRegionIteratorWithIndex.h"
#include <queue>
#include <math.h>

template <class TI, class TO>
void engeLocalClassConnectivityIndexFilter<TI, TO>::GenerateInputRequestedRegion() {
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
void engeLocalClassConnectivityIndexFilter<TI, TO>::ThreadedGenerateData(const OutputImageRegionType& outputRegionForThread, itk::ThreadIdType threadId) {

  InputImagePointerType inputPtr = const_cast<TI *>(this->GetInput(0));

  InputImageRegionType inputRegionForThread;
  this->CallCopyOutputRegionToInputRegion(inputRegionForThread, outputRegionForThread);
  itk::ImageRegionIteratorWithIndex<TO> outputIt(this->GetOutput(), outputRegionForThread);
  itk::ProgressReporter progress(this, threadId, outputRegionForThread.GetNumberOfPixels());

  InputImageRegionType bufferRegionForThread;
  this->CallCopyOutputRegionToInputRegion(bufferRegionForThread, outputRegionForThread);
  InputImageSizeType maxRad;
  maxRad[0] = m_radius;
  maxRad[1] = m_radius;
  bufferRegionForThread.PadByRadius(maxRad);
  bufferRegionForThread.Crop(inputPtr->GetLargestPossibleRegion());

  // Build a temporary image of chars for use in the flood algorithm
  itk::Size<2> neighborSize;
  neighborSize.Fill(m_radius * 2 + 1);
  itk::Index<2> indexZero;
  indexZero.Fill(0);
  itk::Index<2> middleIndex;
  middleIndex.Fill(m_radius);
  itk::ImageRegion<2> neighborRegion(indexZero, neighborSize);
  TTempImage::Pointer tempBuff = TTempImage::New();
  tempBuff->SetRegions(neighborRegion);
  tempBuff->Allocate();

  itk::Offset<2> offsetZero;
  offsetZero.Fill(0);

  std::queue< itk::Offset<2> > offsetStack;
  engeSimpleNeighborhoodIterator<2> nIt(m_FullyConnected);
  itk::ImageRegionConstIterator<TTempImage> countIt(tempBuff, neighborRegion);

  // Loop on the pixels
  for (outputIt.GoToBegin(); !outputIt.IsAtEnd(); ++outputIt) {

    itk::Index<2> origin = outputIt.GetIndex(); // origin point = current pixel position
    InputPixelType pixelValue = inputPtr->GetPixel(origin);

    //if(pixelValue < m_min || pixelValue > m_max) {
    //  outputIt.Set( 0 );
    //  continue;
    //}

    // initialize the flood algo
    tempBuff->FillBuffer(0);
    tempBuff->SetPixel(middleIndex, 2); // self is inside the area
    offsetStack.push(offsetZero);

    // loop on steps for flood algo
    while(! offsetStack.empty() ) {
      const itk::Offset<2> & stackOffset = offsetStack.front();
      // loop on the (4 or 9) neighbors pixels of the stacked pixel
      for(nIt.GoToBegin(); ! nIt.IsAtEnd(); ++nIt) {
        const itk::Offset<2> offset = stackOffset + (*nIt);
        itk::Index<2> imageIndex = origin      + offset;
        itk::Index<2> buffIndex  = middleIndex + offset;
 
        // if inside the area of interest and inside the image (for borders)
        // TODO ? neighborRegion.IsInside(buffIndex) into a radius-based test: sum((imageIndex-origin)^2) < radius^2
        if ( neighborRegion.IsInside(buffIndex) && bufferRegionForThread.IsInside(imageIndex) ) {
          if ( tempBuff->GetPixel(buffIndex) == 0 ) { // pixel to test as it is new in the queue
            // if pixel has same class, push it into the queue
            InputPixelType neighValue = inputPtr->GetPixel(imageIndex);
            //if(neighValue >= m_min && neighValue <= m_max) {
            if(neighValue == pixelValue) {
              offsetStack.push(offset);
              tempBuff->SetPixel(buffIndex, 2);
            }
            else { // If the pixel is another class
              // Mark the pixel as outside and remove it from the queue.
              tempBuff->SetPixel(buffIndex, 1);
            }
          }
        }


      }
      offsetStack.pop();
    }

    // count 
    unsigned int count = 0;
    for(countIt.GoToBegin(); ! countIt.IsAtEnd(); ++countIt) {
      if(countIt.Get() == 2)
        count++;
    }

    // store output
    outputIt.Set( count );
  }

}

