#include "engeStepInterpolationFilter.h"
#include "itkImageRegionIterator.h"
#include <map>
#include <math.h>

template <class TI, class TO>
void engeStepInterpolationFilter<TI, TO>::ThreadedGenerateData(
    const typename TO::RegionType &outputRegionForThread, itk::ThreadIdType threadId)
{
    unsigned int nbInput = this->GetInput()->GetNumberOfComponentsPerPixel();
    unsigned int nbOutput = m_output_times.size();
    if (nbInput != m_input_times.size()) {
        if (threadId == 0)
            printf("The number of input bands must match the number of specified times!\n");
        exit(EXIT_FAILURE);
    }

    // construct input pixel vector
    typename TI::RegionType inputRegionForThread;
    this->CallCopyOutputRegionToInputRegion(inputRegionForThread, outputRegionForThread);
    itk::ImageRegionIterator<TO> oIt(this->GetOutput(), outputRegionForThread);
    itk::ImageRegionConstIterator<TI> iIt(this->GetInput(), inputRegionForThread);
    typename TI::PixelType pixelI(nbInput);
    typename TO::PixelType pixelO(nbOutput);
    typename TO::PixelType pixelNaN(nbOutput);
    std::map<double, typename TI::InternalPixelType> ordered_idx;
    typename std::map<double, typename TI::InternalPixelType>::const_iterator nextyy;

    for (int i = 0; i < nbOutput; ++i)
        pixelNaN[i] = m_NaN;

    // loop over pixels
    for (oIt.GoToBegin(), iIt.GoToBegin(); !oIt.IsAtEnd(); ++oIt, ++iIt) {
        // Load and map valid data
        pixelI = iIt.Get();
        ordered_idx.clear();
        for (int i = 0; i < nbInput; ++i) {
            if (pixelI[i] != m_NaN && pixelI == pixelI)
                ordered_idx[m_input_times[i]] = pixelI[i];
        }

        // init output to all-no-data
        pixelO = pixelNaN;

        // if not at least 2 valid data, keep nothing
        if (ordered_idx.size() > 1) {

            // interpolate
            unsigned int idx = 0;
            for (double time : m_output_times) {
                // find next greater or = to time     //rem: O(n log(n)), can be reduced to O(n)
                // with a double moving idx, but more bug-prone //TODO
                nextyy = ordered_idx.lower_bound(time);
                const double next_time = nextyy->first;
                const typename TI::InternalPixelType next_val = nextyy->second;

                // before or at begining of data
                if (nextyy == ordered_idx.begin()) {
                    // if (next_time == time) // exact match (else: keep no-data)
                    pixelO[idx] = next_val;
                }

                // we are behind any input value
                else if (nextyy == ordered_idx.end()) {
                    pixelO[idx] = ordered_idx.rbegin()->second;
                }

                // we are in the range of inputs
                else {

                    // get previous
                    const double last_time = (--nextyy)->first;
                    const typename TI::InternalPixelType last_val = nextyy->second;

                    // check if the period is not too large (if a max is specified)
                    if (m_max_dist && (next_time - last_time > m_max_dist) ||
                        m_window_radius && (time - last_time > m_window_radius ||
                                            next_time - time > m_window_radius)) {
                        // do nothing, keep NaN
                    } else {
                        // interpolate
                        double interp = last_val + (time - last_time) * (next_val - last_val) /
                                                       (next_time - last_time);
                        pixelO[idx] = m_round ? round(interp) : interp;
                    }
                }

                idx++;
            }
        }

        // save the result
        oIt.Set(pixelO);
    }
}

template <class TI, class TO>
void engeStepInterpolationFilter<TI, TO>::GenerateOutputInformation()
{
    Superclass::GenerateOutputInformation();
    this->GetOutput()->SetNumberOfComponentsPerPixel(m_output_times.size());
}
