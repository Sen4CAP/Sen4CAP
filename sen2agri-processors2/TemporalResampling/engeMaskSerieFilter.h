#ifndef _ENGE_MASK_FILTER
#define _ENGE_MASK_FILTER

#include "otbMacro.h"
#include "itkImageToImageFilter.h"
#include <vector>

template <class TI, class TO, class TM>
class ITK_EXPORT engeMaskSerieFilter : public itk::ImageToImageFilter<TI, TO>
{
public:
  // Standard typedefs and macros
  typedef engeMaskSerieFilter Self;
  typedef itk::ImageToImageFilter<TI, TO> Superclass;
  typedef itk::SmartPointer<Self> Pointer;
  typedef itk::SmartPointer<const Self> ConstPointer;
  itkNewMacro(Self);
  itkTypeMacro(engeMaskSerieFilter, itk::ImageToImageFilter);

  void setMasks(std::vector<typename TM::InternalPixelType> masked_values, std::vector<typename TO::InternalPixelType> replacing_value) {
    m_masked_values = masked_values; m_replacing_values = replacing_value;
  }
  void setMask(typename TM::InternalPixelType masked_value, typename TO::InternalPixelType replacing_value) {
    m_masked_values.clear(); m_replacing_values.clear(); addMask(masked_value, replacing_value);
  }
  void addMask(typename TM::InternalPixelType masked_value, typename TO::InternalPixelType replacing_value) {
    m_masked_values.push_back(masked_value); m_replacing_values.push_back(replacing_value);
  }
  void setInvertedMode(bool invert) { m_invert = invert; }

protected:
  engeMaskSerieFilter() : m_masked_values(), m_replacing_values(), m_invert(false) {}
  virtual ~engeMaskSerieFilter() {}
  void ThreadedGenerateData(const typename TO::RegionType & outputRegionForThread, itk::ThreadIdType threadId);
  void GenerateOutputInformation();

private:
  // copy operators purposely not implemented
  engeMaskSerieFilter(const Self &);
  void operator=(const Self&);

  // size = nbMasks
  std::vector<typename TM::InternalPixelType> m_masked_values; 
  // size = nbBands * nbMasks
  std::vector<typename TO::InternalPixelType> m_replacing_values;
  bool m_invert;
  
};

#  ifndef ITK_MANUAL_INSTANTIATION
#    include "engeMaskSerieFilter.txx"
#  endif

#endif




