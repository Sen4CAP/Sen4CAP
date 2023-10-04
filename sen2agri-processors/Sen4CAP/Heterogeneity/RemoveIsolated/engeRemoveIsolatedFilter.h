#ifndef __CCI_MAJORITY_WITHOUT_WATER__
#define __CCI_MAJORITY_WITHOUT_WATER__
 
#include "itkConnectedComponentAlgorithm.h"
#include "itkConditionalConstIterator.h"
#include <vector>

template <class TI, class TO>
class ITK_EXPORT engeRemoveIsolatedFilter: public itk::ImageToImageFilter<TI, TO> {
 public:
  typedef engeRemoveIsolatedFilter Self;
  typedef itk::ImageToImageFilter<TI, TO> Superclass;
  typedef itk::SmartPointer<Self>                            Pointer;
  typedef itk::SmartPointer<const Self>                      ConstPointer;
  itkNewMacro(Self);
  itkTypeMacro(engeRemoveIsolatedFilter, itk::ImageToImageFilter);
  typedef typename TI::ConstPointer InputImageConstPointerType;
  typedef typename TI::Pointer InputImagePointerType;
  typedef typename TI::RegionType   InputImageRegionType;
  typedef typename TI::PixelType    InputPixelType;
  typedef typename TI::SizeType     InputImageSizeType;
  typedef typename TO::Pointer     OutputImagePointerType;
  typedef typename TO::RegionType  OutputImageRegionType;
  typedef typename TO::PixelType   OutputPixelType;
  typedef typename TO::InternalPixelType   OutputInternalPixelType;
  typedef itk::Image< unsigned char, 2 > TTempImage;

  void setRadius(int radius) { m_radius = radius; }
  void setMinNeighbors(int minNeighbors) { m_minNeighbors = minNeighbors; }
  void setNaN(int nan) { m_NaN = nan; }
  void setClasses(std::vector<typename TI::InternalPixelType> &classes) { m_classes = classes; }

 protected:
  engeRemoveIsolatedFilter() : m_radius(1), m_minNeighbors(1), m_NaN(0), m_classes() {}
  virtual ~engeRemoveIsolatedFilter() {}
  void ThreadedGenerateData(const OutputImageRegionType& outputRegionForThread, itk::ThreadIdType threadId);
  void GenerateInputRequestedRegion();

 private:
  engeRemoveIsolatedFilter(const Self &); //purposely not implemented
  void operator =(const Self&); //purposely not implemented

  int m_radius, m_minNeighbors, m_NaN;
  std::vector<typename TI::InternalPixelType> m_classes;
};

#ifndef OTB_MANUAL_INSTANTIATION
#include "engeRemoveIsolatedFilter.txx"
#endif

#endif
