#ifndef __LW_SIMPLE_NEIGHBORHOOD_ITERATOR__
#define __LW_SIMPLE_NEIGHBORHOOD_ITERATOR__
 
//#include <unordered_set> // for c++11, not c++98
#include <set>
#include "itkOffset.h"

template <unsigned int VDimension = 2>
class ITK_EXPORT engeSimpleNeighborhoodIterator  {
 public:
  typedef itk::Offset< VDimension > OffsetType;
  struct offsetLessThan {
    bool operator()(const OffsetType &a, const OffsetType &b) const {
      for(unsigned int dim = 0; dim < VDimension; ++dim)
        if(a[dim] < b[dim])
          return true;
      return false;
    }
  };
  typedef std::set<OffsetType, offsetLessThan> ContainerType;

  engeSimpleNeighborhoodIterator(bool fully = false) : m_fullyConnected(fully) {
    init();
  }
  ~engeSimpleNeighborhoodIterator() {}

  void setFullConnection(bool fullyConnected) { m_fullyConnected = fullyConnected; init(); }
  void GoToBegin() { it = set.begin(); } //c++11: cbegin
  bool IsAtEnd() { return it == set.end(); } //c++11: cend
  void operator++() { it++; }
  void operator--() { it--; }
  OffsetType operator*() { return *it; }

 private:
 
  void init() {
    OffsetType offset;
    set.clear();
    if(m_fullyConnected) {
      // activate all neighbors that are face+edge+vertex, do not include the center pixel
      init_helper(0, offset);
      offset.Fill(0);
      set.erase(offset);
    } else {
      // only activate the neighbors that are face connected
      offset.Fill(0);
      for ( unsigned int d = 0; d < VDimension; ++d ) {
        offset[d] = -1;
        set.insert(offset);
        offset[d] = 1;
        set.insert(offset);
        offset[d] = 0;
      }
    }
    GoToBegin();
  }

  void init_helper(int dim, OffsetType offset) {
    if(dim >= VDimension)
      set.insert(offset);
    else {
      for ( int r = -1; r <= 1; r++ ) {
        offset[dim] = r;
        init_helper(dim+1, offset);
      }
    }
  }

  bool m_fullyConnected;
  typename ContainerType::const_iterator it;
  ContainerType set;
};

#endif
