#!/usr/bin/env python

import argparse
from collections import defaultdict
from datetime import date
import datetime as dt
# from datetime import datetime
from datetime import timedelta
from glob import glob
import multiprocessing.dummy
import os
import os.path
from osgeo import osr, gdal, ogr
from gdal import gdalconst
import re
import sys
import csv
import errno
import numpy as np
from itertools import islice
from pyarrow import ipc
from scipy.signal import savgol_filter
from scipy.interpolate import interp1d
from pandas import isnull as isnull

METRICS_COLUMN_SUFFIXES=["mean_LaiSGWinter", "sum_LaiSGInt0", "sum_LaiSGInt1", "sum_LaiSGInt2", "max_SG", "daymax_SG", "max_LAI"]
INDICES_COLUMN_SUFFIXES=["Ind_MaxLai", "Ind_HalfLai", "Ind_Emerg", "Ind_EndLai"]

ID_COL_NAME = "NewID"

class OutputsHandler(object) :
    def __init__(self, sg_writer, indices_writer, metrics_writer):
        self.sg_writer = sg_writer
        self.indices_writer = indices_writer
        self.metrics_writer = metrics_writer
        
        
    def write_sg_rows(self, rows) :
        self.sg_writer.writerows(rows)
    
    def write_indices_rows(self, rows) :
        self.indices_writer.writerows(rows)
    
    def write_metrics_rows(self, rows) :
        self.metrics_writer.writerows(rows)
    

class SelectedColumns(object):
    def __init__(self, col_names, global_col_indices, id_col_global_idx, id_col_name = ID_COL_NAME):
        
        col_names.sort()
        self.columns = col_names
        self.global_col_indices = global_col_indices
        self.id_col_global_idx = id_col_global_idx
        
        dates = []
        self.id_col_name = id_col_name
        self.mean_indices = []
        self.valid_pix_indices = []
        self.total_pix_indices = []
        cur_idx = 1 # We start from 1 as on the first position in data is the ID
        for col in col_names:
            # get the date until the first _
            idx = col.index('_')
            if idx > 0:
                date_str = col[:idx]
                date_time_obj = dt.datetime.strptime(date_str, '%Y%m%d').date()
                if not date_time_obj in dates:
                    dates.append(date_time_obj)  
                    
            if "_mean_" in col:
                self.mean_indices.append(cur_idx)
                
            if "_valid_pixels_cnt_" in col:
                self.valid_pix_indices.append(cur_idx)
                
            if "_total_pixels_cnt_" in col:
                self.total_pix_indices.append(cur_idx)

            cur_idx = cur_idx+1
            
        self.dates = np.array(dates)
        self.all_column_names = [self.id_col_name] + self.columns
        
        # print(self.mean_indices)
            
    def get_all_columns(self) :
        return self.all_column_names
        
    def get_all_global_col_indices(self) :
        return [self.id_col_global_idx] + self.global_col_indices

def NoneInt(x):
    if x!=None:
        x=int(x)
    return x

def SGInterp(DOY2, lai2, xDoy):
    Flinear = interp1d(DOY2, lai2, kind='linear')
    savgol = savgol_filter(Flinear(xDoy), window_length=21, polyorder=1)
    return savgol

def SavitzkyGolay(cropfield, dates, bi_vals, DoyList, Date0):
    LAI   = bi_vals
    savgol = np.ones(len(DoyList))*np.nan
    if len(LAI)>1:  
        # print("Starting SG for id = {}, dates = {}, values = {}".format(cropfield, dates, bi_vals))
        print("Starting SG for id = {}".format(cropfield))
        Date = dates
        doy = np.array([(x - Date0).days  for x in Date])
        test = (DoyList>=min(doy))&(DoyList<=max(doy))
        if np.sum(test)>=21:
            print("Interpolating ...")
            Flinear = interp1d(doy, LAI, kind='linear')
            dum = savgol_filter(Flinear(DoyList[test]), window_length=21, polyorder=1)
            savgol[test] = dum
            print("Interpolating done")
        print("SG done")
    
    savgol[isnull(savgol)] = np.nan
    savgol[(savgol<1e-10)&(savgol>-1e-10)]=0
    savgol = savgol.astype(float)
    savgol = np.where(np.isnan(savgol), None, savgol)

    return savgol

def get_selected_columns(columns) : 
    # print ("Schema: {}".format(reader.schema))
    # bi_cols = []
    bi_cols = ['_mean_LAI', '_valid_pixels_cnt_LAI']
    # bi_cols = ['_mean_LAI', '_valid_pixels_cnt_LAI', '_total_pixels_cnt_LAI']
    # bi_cols = ['_mean_LAI', '_stdev_LAI', '_valid_pixels_cnt_LAI']
    col_names = []
    # columns_to_select = []
    cur_idx = 0
    global_col_indices = []
    id_col_global_idx = -1
    for name in columns:
        if name != ID_COL_NAME:
            for bi_col in bi_cols:
                if bi_col in name:
                    col_names.append(name)
                    global_col_indices.append(cur_idx)
        else :
            id_col_global_idx = cur_idx
        cur_idx = cur_idx+1

    # print("Selected columns: {}".format(col_names))
    return SelectedColumns(col_names, global_col_indices, id_col_global_idx, ID_COL_NAME)

def handle_ipc_file(input, output_handler, DoyList, Date0) :
    reader = ipc.open_file(input)
    
    print("Having a number of {} columns ...".format(len(reader.schema.names)))
    selCols = get_selected_columns(reader.schema.names)
    all_column_names = selCols.get_all_columns()
    print ("Column names to select: {}".format(all_column_names))
            
    rowcnt = 0
    for i in range(0, reader.num_record_batches):
        b = reader.get_batch(i)
        schema = b.schema
        columns_to_select = []        
        for name in all_column_names:
            columns_to_select.append(b.column(schema.get_field_index(name)))

        # print("Columns: {}, num_cols = {}, rows = {}".format(b.schema, b.num_columns, b.num_rows))
        # print("{}".format(b.column(0)))
        rb = b.from_arrays(columns_to_select, all_column_names)
        pd = rb.to_pandas()
        all_cropfields = pd.to_numpy()
        # print("Result: {}".format(all_cropfields))
        handle_batch_record(selCols, all_cropfields, DoyList, Date0, output_handler)
        
def handle_csv_file(input, output_handler, DoyList, Date0) :
    with open(input, 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = csv.reader(read_obj)
        header = next(csv_reader)
        selCols = get_selected_columns(header)
        all_column_names = selCols.get_all_columns()
        
        N = 1000
        # Extract the relevant columns
        if header != None:
            while True:
                gen = islice(read_obj,N)
                arr = np.genfromtxt(gen, delimiter=',', usecols=selCols.get_all_global_col_indices(), encoding=None)
                all_cropfields = np.array(arr.tolist())
                # all_cropfields = arr.view(np.float).reshape(arr.shape + (-1,))
                handle_batch_record(selCols, all_cropfields, DoyList, Date0, output_handler)
                if arr.shape[0]<N:
                    break

def compute_crop_partitioning_indices(SG) :

    # Extract an array with IndMaxLai IndHalfLai IndEmerg IndEndLai
    print("Computing crop partitioning indices ...")
    dum, = np.where(SG!=None)
    # print(dum)
    if len(dum)<50:
        print("Cannot extract indices as the length of SG array is {}".format(len(dum)))
        return None
        # return [None, None, None, None]
    else:
        FirstData = dum[0]
        SG[:FirstData] = SG[FirstData]

    dum, = np.where(SG==None)
    if len(dum)>0:
        LastData = dum[0]-1
        SG[LastData:] = SG[LastData]

    Max  = np.max(SG)
    # print("MAXimum LAI value in the array = {}".format(Max))
    
    IndMaxLai  = NoneInt(np.argmax(SG))
    # print("IndMaxLai (index of the maximum LAI value in the array) = {}".format(IndMaxLai))
    
    # import pdb; pdb.set_trace()
    if IndMaxLai==0:
        IndHalfLai=0
        IndEmerg = 0
    else:
        # minimum LAI value from index 0 to index of Max LAI
        MinEmerg = np.min(SG[:IndMaxLai])
        # print("MinEmerg = {}".format(MinEmerg))
        # SG.max() = Max (above computed)
        # print("SG.max() = {}".format(SG.max()))
        # Get all values greater than half of the LAI maximum value in array
        # dum, = np.where(SG>SG.max()/2)
        # print("dum1 = {}".format(dum))
        # get the indices of the values before the maximum index whose difference with min emegence LAI  > half of the max - min LAI
        dum, = np.where((SG[:IndMaxLai]-MinEmerg)/(Max-MinEmerg)>.5)
        # print("dum2 = {}".format(dum))
        # First value is considered half of LAI
        IndHalfLai = NoneInt(dum[0])
        # print("IndHalfLai = {}".format(IndHalfLai))
        # get the indices before half of LAI whose difference with min emegence LAI  < 10 % of the max - min LAI 
        dum, = np.where((SG[:IndHalfLai]-MinEmerg)/(Max-MinEmerg)<.1)
        # print("dum3 = {}".format(dum))
        if len(dum)>0:
            IndEmerg = NoneInt(dum[-1]+1)
        else:
            IndEmerg = 0
        # Next value having LAI - min emergence LAI > 10 % of the max - min LAI 
        # print("IndEmerg = {}".format(IndEmerg))
            
    if IndMaxLai==333:
        IndEndLai=333
    else:
        # Minimum LAI value for senescence - the minimum value after the maximum LAI index
        MinSensc = np.min(SG[IndMaxLai:])
        # print("MinSensc = {}".format(MinSensc))
        if MinSensc==Max:
            IndEndLai=333
        else:
            # Get all the values after the maximum LAI whose difference with min senescence LAI  < 10% of Max - minimum senescence LAI 
            dum, = np.where((SG[IndMaxLai:]-MinSensc)/(Max-MinSensc)<.1)
            # print("dum4 = {}".format(dum))
            IndEndLai = NoneInt(IndMaxLai + dum[0])
    
    # print("IndEndLai = {}".format(IndEndLai)) 
    print("Crop partitioning indices computation done!")
    return [IndMaxLai, IndHalfLai, IndEmerg, IndEndLai]

def build_lai_metrics_output_record(savgol, indices, orig_lai_max_val) :
    # indices = [IndMaxLai, IndHalfLai, IndEmerg, IndEndLai]
    IndMaxLai = indices[0]
    IndHalfLai = indices[1]
    IndEmerg = indices[2]
    IndEndLai = indices[3]
    
    MeanLaiSGWinter = np.mean(savgol[0:IndEmerg+1 ])
    SumLaiSGInt0 = np.sum(savgol[IndEmerg  :IndHalfLai+1])
    SumLaiSGInt1 = np.sum(savgol[IndHalfLai:IndMaxLai +1] )
    SumLaiSGInt2 = np.sum(savgol[IndMaxLai :IndEndLai +1] )
    Laimax  = orig_lai_max_val
    LaiSGmax  = np.max(savgol)
    LaiSGdaymax  = np.argmax(savgol)

    # outputs :=["mean_LaiSGWinter", "sum_LaiSGInt0", "sum_LaiSGInt1", "sum_LaiSGInt2", "max_SG", "daymax_SG", "max_LAI"]
    return [MeanLaiSGWinter, SumLaiSGInt0, SumLaiSGInt1, SumLaiSGInt2, LaiSGmax, LaiSGdaymax, Laimax]
    
def handle_batch_record(selCols, all_cropfields, DoyList, Date0, output_handler):
    sg_outputs = []
    indices_outputs = []
    metrics_outputs = []
    for cropfield_descr in all_cropfields:
        # print("cropfield_descr: {}".format(cropfield_descr))
        mean_vals = cropfield_descr[selCols.mean_indices]
        valid_pixels = cropfield_descr[selCols.valid_pix_indices]
        # total_pixels = cropfield_descr[selCols.total_pix_indices]
        total_pixels = valid_pixels # TODO: For now we go with this but the line above should be uncommented
        
        # TODO: Threshold should be configurable
        test = np.array([y>0.8*x for x,y in zip(total_pixels,valid_pixels)])

        # print ("Test = {}".format(test))
        if (len(test) == 0) :
            continue
        
        dates       =  selCols.dates[test]
        bi_vals     =  mean_vals[test] / 1e3
        
        cropfield = cropfield_descr[0]
        savgol = SavitzkyGolay(cropfield, dates, bi_vals, DoyList, Date0)
        
        cropfield_n = int(cropfield)
        # add the output to the output lines
        sg_outputs.append( [cropfield_n] + list(savgol) )

        # compute the crop partitioning indices
        indices = compute_crop_partitioning_indices(savgol)
        if indices is not None:
            # add the indices to the output list
            indices_outputs.append([cropfield_n] + indices)
            
            # create the output for the metrics
            metrics_output = build_lai_metrics_output_record(savgol, indices, np.max(bi_vals))
            metrics_outputs.append([cropfield_n] + metrics_output)
    
    output_handler.sg_writer.writerows(sg_outputs)
    output_handler.indices_writer.writerows(indices_outputs)
    output_handler.metrics_writer.writerows(metrics_outputs)

def build_sg_output_header(date_list) :
    header = [ID_COL_NAME]
    header += [x.strftime("%Y%m%d") + "_LAI_SG" for x in date_list]
    
    return header

def build_indices_output_header(first_date) :
    header = [ID_COL_NAME]
    first_date_str = first_date.strftime("%Y%m%d")
    for suffix in INDICES_COLUMN_SUFFIXES:
        header += [first_date_str + "_" + suffix]
    
    return header

def build_metrics_output_header(first_date) :
    header = [ID_COL_NAME]
    first_date_str = first_date.strftime("%Y%m%d")
    for suffix in METRICS_COLUMN_SUFFIXES:
        header += [first_date_str + "_" + suffix]
    
    return header

def handle_file(input, output_handler, year):
    # TODO: These dates should be configurables (or at least the end date)
    Date0 = dt.date(int(year), 1, 1)
    DateEnd = dt.date(int(year), 11, 30)
    DateList = [Date0 + dt.timedelta(days=x) for x in range(0,(DateEnd-Date0).days   + 1,1)]
    DateList=np.unique(DateList)
    DoyList = np.array([(x-Date0 ).days for x in DateList])
    
    # build the headers
    sg_header = build_sg_output_header(DateList)
    indices_header = build_indices_output_header(DateList[0])
    metrics_header = build_metrics_output_header(DateList[0])
    # write the headers
    output_handler.write_sg_rows([sg_header])
    output_handler.write_indices_rows([indices_header])
    output_handler.write_metrics_rows([metrics_header])
    
    lcinput = input.lower()
    if lcinput.endswith('.ipc'):
        handle_ipc_file(input, output_handler, DoyList, Date0)
    elif lcinput.endswith('.csv'):
        handle_csv_file(input, output_handler, DoyList, Date0)
    else :
        print("Invalid file type received as input (unknow extension for {})".format(input))
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Performs the Savitzky Golay interpolation of the BI values in received file"
    )
    parser.add_argument("-i", "--input", help="Input extracted features (mean, stddev etc.) for the desired BI", required=True)
    parser.add_argument("-s", "--sg-output", help="Output file containing SG LAI interpolated values", required=True)
    parser.add_argument("-x", "--indices-output", help="Output file for crop growth indices extracted values", required=True)
    parser.add_argument("-m", "--metrics-output", help="Output file LAI metrics extracted values", required=True)
    parser.add_argument("-y", "--year", help="The processing year", required=True)
    
    args = parser.parse_args()
 
    with open(args.sg_output, "w") as sg_file, open(args.indices_output, "w") as indices_file, open(args.metrics_output, "w") as metrics_file:
        output_handler = OutputsHandler(csv.writer(sg_file, quoting=csv.QUOTE_MINIMAL), 
                                        csv.writer(indices_file, quoting=csv.QUOTE_MINIMAL),
                                        csv.writer(metrics_file, quoting=csv.QUOTE_MINIMAL))
        
        handle_file(args.input, output_handler, args.year)
    
if __name__ == "__main__":
    main()
