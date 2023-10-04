#include "itkImageRegionIterator.h"
#include "engeMaskSerieFilter.h"

// input : 
//   0: input multi(or mono)-band
//   1: mask (mmultiband or first band)
// output :
//   same as input, with masked values modified
template <class TI, class TO, class TM>
void engeMaskSerieFilter<TI, TO, TM>
::ThreadedGenerateData(const typename TO::RegionType& outputRegionForThread, itk::ThreadIdType threadId)
{
  unsigned int nbBands = this->GetInput(0)->GetNumberOfComponentsPerPixel();
  unsigned int nbMasks = m_masked_values.size();

  if(nbBands != this->GetInput(1)->GetNumberOfComponentsPerPixel()) {
    printf("The number of bands of the data serie must match the number for the mask serie !\n");
    exit(EXIT_FAILURE);
  }

  if(nbMasks == 0 || m_replacing_values.size() == 0) {
    printf("specify the masked values and their corresponding replacing values !\n");
    exit(EXIT_FAILURE);
  }
  if(m_replacing_values.size() != nbMasks) {
    printf("specify the vector of replacing values per mask !\n");
    printf("nbMasks = %d, nbReplacingValues = %d\n", nbMasks, (int)m_replacing_values.size());
    exit(EXIT_FAILURE);
  }

  // initialize iterators
  typename TI::RegionType inputRegionForThread;
  this->CallCopyOutputRegionToInputRegion(inputRegionForThread, outputRegionForThread);
  itk::ImageRegionIterator     <TO> oIt(this->GetOutput(), outputRegionForThread);
  itk::ImageRegionConstIterator<TI> iIt(this->GetInput(0), inputRegionForThread);
  itk::ImageRegionConstIterator<TI> mIt(this->GetInput(1), inputRegionForThread); // rem: should be TM but not allowed by ImageToImageFilter

  typename TO::PixelType pixelO(nbBands);
  typename TO::PixelType pixelM(nbBands);

  // loop over pixels
  if(!m_invert) {

    for(oIt.GoToBegin(), mIt.GoToBegin(), iIt.GoToBegin(); ! oIt.IsAtEnd(); ++oIt, ++mIt, ++iIt)
    {
      pixelM = mIt.Get();
      pixelO = iIt.Get();
      for(int m = 0; m < nbMasks; ++m) {
        for(int i = 0; i < nbBands; ++i) {
          if(pixelM[i] == m_masked_values[m]) {
            pixelO[i] = m_replacing_values[m];
            break;
          }
        }
      }
      oIt.Set( pixelO );
    }

  } else { // inverted mode: specified values are not changed

    for(oIt.GoToBegin(), mIt.GoToBegin(), iIt.GoToBegin(); ! oIt.IsAtEnd(); ++oIt, ++mIt, ++iIt)
    {
      pixelM = mIt.Get();
      pixelO = iIt.Get();
      for(int i = 0; i < nbBands; ++i) {
        bool masked = true;
        for(int m = 0; m < nbMasks; ++m) {
          if(pixelM[i] == m_masked_values[m]) {
            masked = false;
            break;
          }
        }
        if(masked) {
          pixelO[i] = m_replacing_values[0]; // only the 1st specified value is used
        }
      }
      //printf("m: %d not in %d %d %d (change=%d), i: %d, o: %d\n", mask, m_masked_values[0], m_masked_values[1], m_masked_values[2], change, iIt.Get()[0], pixelO[0]);
      oIt.Set( pixelO );
    }

  }

}

template <class TI, class TO, class TM>
void engeMaskSerieFilter<TI, TO, TM>
::GenerateOutputInformation()
{
  unsigned int nbBands = this->GetInput(0)->GetNumberOfComponentsPerPixel();
  Superclass::GenerateOutputInformation();
  this->GetOutput()->SetNumberOfComponentsPerPixel( nbBands );

}


