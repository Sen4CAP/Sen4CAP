#include "itkImageRegionIterator.h"
#include "engeStepInterpolationFilter.h"
#include "lwString.h"
#include <math.h>
#include <map>
#include <signal.h>


template <class TI, class TO>
void engeStepInterpolationFilter<TI, TO>
::ThreadedGenerateData(const typename TO::RegionType& outputRegionForThread, itk::ThreadIdType threadId)
{
  unsigned int nbInput = this->GetInput()->GetNumberOfComponentsPerPixel();
  unsigned int nbOutput = 1 + std::floor((m_end - m_start) / m_step);
  if(nbInput != m_times.size()) {
    if(threadId == 1) printf("The number of input bands must match the number of specified times !\n");
    //exit(EXIT_FAILURE);
    kill(getpid(), SIGABRT); //SIGTERM, SIGKILL);
  }

  // construct input pixel vector
  typename TI::RegionType inputRegionForThread;
  this->CallCopyOutputRegionToInputRegion(inputRegionForThread, outputRegionForThread);
  itk::ImageRegionIterator     <TO> oIt(this->GetOutput(), outputRegionForThread);
  itk::ImageRegionConstIterator<TI> iIt(this->GetInput(), inputRegionForThread);
  typename TI::PixelType pixelI(nbInput);
  typename TO::PixelType pixelO(nbOutput);
  typename TO::PixelType pixelNaN(nbOutput);
  std::map<double, typename TI::InternalPixelType> ordered_idx;
  typename std::map<double, typename TI::InternalPixelType>::const_iterator nextyy;

  for(int i = 0; i < nbOutput; ++i)
    pixelNaN[i] = m_NaN;

  bool noMinMax = ! ((m_min == m_min) && (m_max == m_max));

  // loop over pixels
  for(oIt.GoToBegin(), iIt.GoToBegin(); ! oIt.IsAtEnd(); ++oIt, ++iIt)
  {
    // Load and map valid data
    pixelI = iIt.Get();
    ordered_idx.clear();
    for(int i = 0; i < nbInput; ++i)
      if(pixelI[i] != m_NaN && pixelI[i] == pixelI[i])
        if(noMinMax || (pixelI[i] >= m_min && pixelI[i] <= m_max))
          ordered_idx[m_times[i]] = pixelI[i];

    // init output to all-no-data
    pixelO = pixelNaN;

    // if not at least 2 valid data, keep nothing
    if(ordered_idx.size() > 1) {

      // interpolate
      unsigned int idx = 0;
      for(double time = m_start; time <= m_end; time += m_step) {
        // debug check
        if(idx >= nbOutput) {
          printf("Too high idx !!\n");
          exit(EXIT_FAILURE);
        }


        // find next greater or = to time     //rem: O(n log(n)), can be reduced to O(n) with a double moving idx, but more bug-prone //TODO
        nextyy = ordered_idx.lower_bound(time);
        const double next_time = nextyy->first;
        const typename TI::InternalPixelType next_val = nextyy->second;

        // before or at begining of data
        if(nextyy == ordered_idx.begin()) {
          if(next_time == time) // exact match (else: keep no-data)
            pixelO[idx] = next_val;
        }

        // we are behind any input value
        else if(nextyy == ordered_idx.end()) {
          // do nothing, keep NaN
        }

        // we are in the range of inputs
        else {

          // get previous
          const double last_time = (--nextyy)->first;
          const typename TI::InternalPixelType last_val = nextyy->second;

          // check if the period is not too large (if a max is specified)
          if(m_max_dist && (next_time - last_time > m_max_dist) ) {
            // do nothing, keep NaN
          } else {
            // interpolate
            double interp = last_val + (time - last_time) * (next_val - last_val) / (next_time - last_time);
            pixelO[idx] = m_round ? round(interp) : interp;
          }

        }

        idx++;
      }

    }

    // save the result
    oIt.Set( pixelO );
  }

}

template <class TI, class TO>
void engeStepInterpolationFilter<TI, TO>
::GenerateOutputInformation()
{
  Superclass::GenerateOutputInformation();
  this->GetOutput()->SetNumberOfComponentsPerPixel( 1 + std::floor((m_end - m_start) / m_step) );
}



