/*
 * Copyright (C) 2005-2017 Centre National d'Etudes Spatiales (CNES)
 *
 * This file is part of Orfeo Toolbox
 *
 *     https://www.orfeo-toolbox.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef otbMarkers1CsvWriter_h
#define otbMarkers1CsvWriter_h

#include "itkProcessObject.h"
#include <utility>
#include <string>

namespace otb {

/** \class Markers1CsvWriter
 *  \brief Write in a text file the values stored in a MeasurementVector set as
 *  input
 *
 * The vector can be set as input via AddInput(name, vector) where name
 * is the name of the statistic, and vector the values.
 * Supported vector types are those implementing the method GetElement(idx)
 * and defining the type ValueType.
 *
 *
 * \ingroup OTBIOXML
 */
template < class TMeasurementVector>
class  Markers1CsvWriter :
    public itk::Object
{
public:
  /** Standard class typedefs */
  typedef Markers1CsvWriter          Self;
  typedef itk::Object                      Superclass;
  typedef itk::SmartPointer< Self >        Pointer;
  typedef itk::SmartPointer<const Self>    ConstPointer;

  /** Run-time type information (and related methods). */
  itkTypeMacro(Markers1CsvWriter, itk::Object);

  /** Method for creation through the object factory. */
  itkNewMacro(Self);

  typedef struct {
     std::vector<double> values;
  } FieldEntriesType;

  typedef struct {
      std::string fid;
      std::vector<FieldEntriesType> fieldsEntries;

  } FileFieldsInfoType;

  class HeaderInfoType {
  public:
      HeaderInfoType() {}
      HeaderInfoType(const std::string &hdr, const std::string &marker, bool isInt, bool isFieldIdCol, int posInHdr) {
          fullColumnName = hdr;
          markerName = marker;
          isFieldIdColumn = isFieldIdCol;
          isIntegerVal = isInt;
          posInHeader = posInHdr;
      }
      std::string GetSimpleMarkerName() const {return markerName;}
      std::string GetFullColumnName() const {return fullColumnName;}
      bool IsIntegerValue() const {return isIntegerVal;}
      bool IsFieldIdColumn() const {return isFieldIdColumn;}
      int GetPositionInHeader() const {return posInHeader;}
    private:
      std::string fullColumnName;
      std::string markerName;
      bool isIntegerVal;
      bool isFieldIdColumn;
      int posInHeader;
  };

  template <typename MapType, typename MapMinMaxType>
  class ColumnToValuesInfo {
  public:
      ColumnToValuesInfo() {}
      ColumnToValuesInfo(const HeaderInfoType &hdrInfo, const std::map<std::string, const MapType*> &mMain,
                             const std::map<std::string, const MapMinMaxType*> &mOpt) {
          hdrItemInfo = hdrInfo;
          const std::string &name = hdrInfo.GetSimpleMarkerName();
          typename std::map<std::string, const MapType*>::const_iterator it1 = mMain.find(name);
          typename std::map<std::string, const MapMinMaxType*>::const_iterator it2 = mOpt.find(name);
          const MapType* mapMain = ((it1 != mMain.end()) ? it1->second : NULL);
          const MapMinMaxType* mapOpt = ((it2 != mOpt.end()) ? it2->second : NULL);
          isMeanField = (name == "mean");
          itMainValid = itOptValid = false;
          if (mapMain != NULL) {
              itMain = mapMain->begin();
              itMainValid = true;
          }
          if (mapOpt != NULL) {
              itOpt = mapOpt->begin();
              itOptValid = true;
          }
      }

      double GetNextValue(int bandIdx) {
          double retVal;
          if (itMainValid) {
              retVal = (isMeanField ? itMain->second.mean[bandIdx] : itMain->second.stdDev[bandIdx]);
              ++itMain;
          } else {
              retVal = itOpt->second[bandIdx];
              ++itOpt;
          }
          return retVal;
      }

  public:
      HeaderInfoType hdrItemInfo;

  private:
      typename MapType::const_iterator itMain;
      typename MapMinMaxType::const_iterator itOpt;
      bool itMainValid;
      bool itOptValid;
      bool isMeanField;
  };

  typedef std::vector<FileFieldsInfoType>         FileFieldsContainer;

  /** Method to add a map statistic with a given type */
  template <typename MapType, typename MapMinMaxType>
  void AddInputMap(const std::map<std::string, const MapType*> &map, const std::map<std::string,
                   const MapMinMaxType*> &mapOptionals);

  void WriteOutputCsvFormat();
  void WriteCsvHeader(std::ofstream &fileStream);
  void WriteEntriesToCsvOutputFile(std::ofstream &outStream, const FileFieldsInfoType &fileFieldsInfos);
  void RemoveDuplicates(std::vector<FieldEntriesType> &fieldsEntries);

  /** Remove previously added inputs (vectors and maps) */
  void CleanInputs();

  /** Trigger the processing */
  void Update()
  {
    this->GenerateData();
  }

  typedef typename std::vector<std::string>        StringVectorType;

  /** Set the output filename */
  itkSetStringMacro(TargetFileName);
  itkGetStringMacro(TargetFileName);

  /** Set the header fields */
  void SetHeaderFields(const std::string &fileName, const StringVectorType &vec,
                       const std::string &idFieldName, bool bIsInteger);

  inline void SetUseStdev(bool bUseStdev) {
      m_bUseStdev = bUseStdev;
  }

  inline void SetUseMinMax(bool bUseMinMax) {
      m_bUseMinMax = bUseMinMax;
  }

  inline void SetUseValidityCnt(bool bValidityCnt) {
      m_bUseValidityCnt = bValidityCnt;
  }

  inline void SetUseMedian(bool bUseMedian) {
      m_bUseMedian = bUseMedian;
  }

  inline void SetUseP25(bool bUseP25) {
      m_bUseP25 = bUseP25;
  }

  inline void SetUseP75(bool bUseP75) {
      m_bUseP75 = bUseP75;
  }

  inline void SetCsvSeparator(char sep) {
      m_csvSeparator = sep;
  }

  inline void SetMapValuesIndex(int idx) {
      m_mapValuesIndex = idx;
  }

  inline void SetDefaultProductType(const std::string &defType) {
      m_defaultPrdType = defType;
  }

protected:

  virtual void GenerateData();

  Markers1CsvWriter();
  ~Markers1CsvWriter() override {}
  void PrintSelf(std::ostream& os, itk::Indent indent) const override;

private:

  std::string BuildHeaderItem(const std::string &dateStr, const std::string &hdrItem, const std::string &fileType,
                    const std::vector<std::string> &additionalFields);
  int GetPositionInHeader(const std::string &name);
  typedef struct {
        void SetIsInt(bool isInt) {m_bIsInt = isInt;}
        bool operator() (const FileFieldsInfoType &i, const FileFieldsInfoType &j) {
            return m_bIsInt ? (atoi(i.fid.c_str()) < atoi(j.fid.c_str())) : (i.fid < j.fid);
        }

        private:
            bool m_bIsInt;
  } FieldsComparator;

  FieldsComparator m_FieldsComparator;
  Markers1CsvWriter(const Self&) = delete;
  void operator=(const Self&) = delete;

  std::string                 m_TargetFileName;
  std::vector<HeaderInfoType>      m_vecHeaderFields;

  bool m_bIdIsInteger;

  FileFieldsContainer           m_FileFieldsContainer;
  bool                          m_bUseMinMax;
  bool                          m_bUseStdev;
  bool                          m_bUseValidityCnt;
  bool                          m_bUseMedian;
  bool                          m_bUseP25;
  bool                          m_bUseP75;

private:
    int m_IdPosInHeader;
    int m_MeanPosInHeader;
    int m_StdevPosInHeader;
    int m_MinPosInHeader;
    int m_MaxPosInHeader;
    int m_ValidPixelsPosInHeader;
    char m_csvSeparator;
    int m_mapValuesIndex;
    std::string m_defaultPrdType;
    int m_MedianPosInHeader;
    int m_P25PosInHeader;
    int m_P75PosInHeader;
}; // end of class Markers1CsvWriter

} // end of namespace otb

#ifndef OTB_MANUAL_INSTANTIATION
#include "otbMarkers1CsvWriter.txx"
#endif

#endif
