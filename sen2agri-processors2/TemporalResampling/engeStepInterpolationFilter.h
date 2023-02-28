#ifndef _ENGE_WHITTAKER_MB_FILTER
#define _ENGE_WHITTAKER_MB_FILTER

#include "itkImageToImageFilter.h"
#include "otbMacro.h"

#include <type_traits>

template <class TI, class TO>
class ITK_EXPORT engeStepInterpolationFilter : public itk::ImageToImageFilter<TI, TO>
{
public:
    // Standard typedefs and macros
    typedef engeStepInterpolationFilter Self;
    typedef itk::ImageToImageFilter<TI, TO> Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;
    itkNewMacro(Self);
    itkTypeMacro(engeStepInterpolationFilter, itk::ImageToImageFilter);

    void setOutputTimes(std::vector<double> output_times)
    {
        m_output_times = output_times;
    }
    void setMaxDist(double max_dist)
    {
        m_max_dist = max_dist;
    }
    void setWindowRadius(double window_radius)
    {
        m_window_radius = window_radius;
    }
    void setInputTimes(std::vector<double> input_times)
    {
        m_input_times = input_times;
    }
    void setNaN(typename TI::InternalPixelType NaN)
    {
        m_NaN = NaN;
    }

protected:
    engeStepInterpolationFilter() : m_NaN(), m_max_dist(), m_window_radius()
    {
        m_round = std::is_integral<typename TO::InternalPixelType>();
    }
    virtual ~engeStepInterpolationFilter()
    {
    }
    void GenerateOutputInformation();
    void ThreadedGenerateData(const typename TO::RegionType &outputRegionForThread,
                              itk::ThreadIdType threadId);

private:
    // copy operators purposely not implemented
    engeStepInterpolationFilter(const Self &);
    void operator=(const Self &);

    bool m_round;
    double m_max_dist, m_window_radius;
    std::vector<double> m_input_times, m_output_times;
    typename TI::InternalPixelType m_NaN;
};

#ifndef ITK_MANUAL_INSTANTIATION
#include "engeStepInterpolationFilter.txx"
#endif

#endif
