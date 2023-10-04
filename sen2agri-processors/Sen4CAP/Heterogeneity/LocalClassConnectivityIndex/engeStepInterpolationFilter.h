#ifndef _ENGE_WHITTAKER_MB_FILTER
#define _ENGE_WHITTAKER_MB_FILTER

#include "otbMacro.h"
#include "lwScalar.h"
#include "itkImageToImageFilter.h"

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

  void setStepper(double start, double step, double end) { m_start= start; m_step = step; m_end = end; }
  void setMaxDist(double max_dist) { m_max_dist = max_dist; }
  void setDataTimes(std::vector<double> times) { m_times = times; }
  void setNaN(typename TI::InternalPixelType NaN) { m_NaN = NaN; }
  void setMinMax(double min, double max) { m_min = min; m_max = max; }

protected:
  engeStepInterpolationFilter() : m_start(0.), m_step(1.), m_end(1.), m_NaN(0), m_times(), m_max_dist(0) {
    m_round = lwScalar::isInteger<typename TO::InternalPixelType>();
    m_min = lwScalar::getNaN<double>();
    m_max = lwScalar::getNaN<double>();
  }
  virtual ~engeStepInterpolationFilter() {}
  void GenerateOutputInformation();
  void ThreadedGenerateData(const typename TO::RegionType & outputRegionForThread, itk::ThreadIdType threadId);

private:
  // copy operators purposely not implemented
  engeStepInterpolationFilter(const Self &);
  void operator=(const Self&);
  
  bool m_round;
  double m_start, m_end, m_step, m_max_dist, m_min, m_max;
  std::vector<double> m_times;
  typename TI::InternalPixelType m_NaN;
};

#  ifndef ITK_MANUAL_INSTANTIATION
#    include "engeStepInterpolationFilter.txx"
#  endif

#endif


