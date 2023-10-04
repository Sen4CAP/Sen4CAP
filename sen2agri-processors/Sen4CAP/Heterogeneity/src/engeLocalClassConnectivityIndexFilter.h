#ifndef __CCI_LOCAL_CONNECTIVITY_INDEX__
#define __CCI_LOCAL_CONNECTIVITY_INDEX__
 
#include "itkConnectedComponentAlgorithm.h"
#include "itkConditionalConstIterator.h"

template <class TI, class TO>
class ITK_EXPORT engeLocalClassConnectivityIndexFilter: public itk::ImageToImageFilter<TI, TO> {
 public:
  typedef engeLocalClassConnectivityIndexFilter Self;
  typedef itk::ImageToImageFilter<TI, TO> Superclass;
  typedef itk::SmartPointer<Self>                            Pointer;
  typedef itk::SmartPointer<const Self>                      ConstPointer;
  itkNewMacro(Self);
  itkTypeMacro(engeLocalClassConnectivityIndexFilter, itk::ImageToImageFilter);
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
  void setFullConnection(bool b) { m_FullyConnected = b; }
//  void setMin(typename TI::InternalPixelType val) { m_min = val; }
//  void setMax(typename TI::InternalPixelType val) { m_max = val; }

 protected:
  engeLocalClassConnectivityIndexFilter() : m_radius(5), m_FullyConnected(false) {} //, m_min(1), m_max(10) {}
  virtual ~engeLocalClassConnectivityIndexFilter() {}
  void ThreadedGenerateData(const OutputImageRegionType& outputRegionForThread, itk::ThreadIdType threadId);
  void GenerateInputRequestedRegion();

 private:
  engeLocalClassConnectivityIndexFilter(const Self &); //purposely not implemented
  void operator =(const Self&); //purposely not implemented

  int m_radius;
  bool m_FullyConnected;
  //InputPixelType m_min, m_max;
};

#ifndef OTB_MANUAL_INSTANTIATION
#include "engeLocalClassConnectivityIndexFilter.txx"
#endif

#endif
