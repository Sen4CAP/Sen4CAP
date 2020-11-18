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

  typedef std::vector<FileFieldsInfoType>         FileFieldsContainer;

  /** Method to add a map statistic with a given type */
  template <typename MapType, typename MapMinMaxType>
  void AddInputMap(const std::string &fileName, const MapType& map, const MapMinMaxType& mapMins, const MapMinMaxType& mapMax,
                   const MapMinMaxType& mapValidPixels, const MapMinMaxType& mapInvalidPixels);

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
  inline StringVectorType GetHeaderFields() {return this->m_HeaderFields;}

  inline void SetUseMinMax(bool bUseMinMax) {
      m_bUseMinMax = bUseMinMax;
  }

  inline void SetCsvSeparator(char sep) {
      m_csvSeparator = sep;
  }

protected:

  virtual void GenerateData();

  Markers1CsvWriter();
  ~Markers1CsvWriter() override {}
  void PrintSelf(std::ostream& os, itk::Indent indent) const override;

private:

  std::string BuildHeaderItem(const std::string &dateStr, const std::string &hdrItem, const std::string &fileType,
                    const std::string &polarisation, const std::string &orbit);
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

  std::vector<std::string>      m_HeaderFields;
  std::string m_idFieldName;
  bool m_bIdIsInteger;

  FileFieldsContainer           m_FileFieldsContainer;
  bool                          m_bUseMinMax;

private:
    int m_IdPosInHeader;
    int m_MeanPosInHeader;
    int m_StdevPosInHeader;
    int m_MinPosInHeader;
    int m_MaxPosInHeader;
    int m_ValidPixelsPosInHeader;
    int m_InvalidPixelsPosInHeader;
    char m_csvSeparator;
}; // end of class Markers1CsvWriter

} // end of namespace otb

#ifndef OTB_MANUAL_INSTANTIATION
#include "otbMarkers1CsvWriter.txx"
#endif

#endif
