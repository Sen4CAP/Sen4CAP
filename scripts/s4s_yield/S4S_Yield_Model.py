#!/usr/bin/env python3
from __future__ import print_function

import argparse

import sys
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.svm import SVR
from mlxtend.feature_selection import SequentialFeatureSelector as SFS
from sklearn.model_selection import train_test_split

from enum import Enum

INPUT_FEATURE_NAMES = ['NewID', 'MeanLaiSGWinter', 'SumLaiSGInt0', 'SumLaiSGInt1', 'SumLaiSGInt2', 'MaxSG', 'DayMaxSG', 'MaxLAI', 'ColdT0','ColdT1','HotT2','SumT1','SumT2','SumT251','SumT252','SumP1','SumP2','SumR1','SumR2','SumE1','SumE2','MeanT1','MeanT2','MeanP1','MeanP2','MeanR1','MeanR2','MeanE1','MeanE2','MeanSW10','MeanSW11','MeanSW12','MeanSW20','MeanSW21','MeanSW22','MeanSW30','MeanSW31','MeanSW32','MeanSW40','MeanSW41','MeanSW42','Yield','d0out','SenBout', 'Trend', 'crop_code']    # TODO: add the other feature names here

class Selection(Enum):
    NoSelection = 1
    Manual = 2
    Automatic = 3

class Algorithm(Enum):
    RandomForest = 1
    LinearRegression = 2
    SVM = 3

class Config(object):
    def __init__(self, args):
        self.input_features = args.input_features
        self.yield_reference = args.yield_reference
        self.statistical_unit_fields = args.statistical_unit_fields
        self.crop_codes = args.crop_codes ### change
        
        # Algorithm Selection
        if args.algo in ['LinearRegression','-MLR','-LM','-LMR','-mlr','-lm','-lmr']:
            self.algo = LinearRegression()
        elif args.algo in ['SupportVectorMachine', '-SVM','-svm']:
            self.algo = SVR(kernel='rbf') 
        else : 
            self.algo = RandomForestRegressor(random_state=0)
        
        self.selection = "none"
        self.selection_suffix = "No_Selection"

        self.manual_selection_features_list = []
        self.selection = Selection.NoSelection
        
        if args.selection in ['Manual', '-M','-m']:
            if not args.manual_selection_features or len(args.manual_selection_features) == 0:
                print("Please provide the list of parameters for the manual selection. Exiting ...")
                exit(1)
            self.selection_suffix = "Manual"
            self.selection = Selection.Manual
            self.manual_selection_features_list = ['yield']
            self.manual_selection_features_list += args.manual_selection_features
            
        elif args.selection in ['Auto','-A','-a']:
            self.selection = Selection.Automatic
            self.selection_suffix = "Automatic"
            self.no_of_selection_features = args.max_automatic_features_no

def remove_ignoring_columns(merged_features, columns_to_ignore) :
    if columns_to_ignore is not None and len(columns_to_ignore) > 0:
        for col_to_ignore in columns_to_ignore:
            if col_to_ignore in merged_features.columns:
                merged_features = merged_features.drop(columns=[col_to_ignore])
            else:
                print("The column to be ignored {} is not present in the input columns list ({})".format(col_to_ignore, merged_features.columns))
    return merged_features
    
def clean_dataset(df):
    assert isinstance(df, pd.DataFrame), "df needs to be a pd.DataFrame"
    df.dropna(inplace=True)
    indices_to_keep = ~df.isin([np.nan, np.inf, -np.inf]).any(axis=1)
    return df[indices_to_keep].astype(np.float64)
    
def train_model(config, calibdata) : 
    # if yield_ref is None:
    #     calibdata = merged_features.iloc[:,1:]
    # else:
    #     calibdata = pd.merge(yield_ref, merged_features,on='NewID').iloc[:,1:]
    # print("Calibration data (initial) = {}".format(calibdata))

    if config.selection is Selection.Manual:
        print("Executing manual selection ...")
        list_features = config.manual_selection_features_list
        calibdata = calibdata.iloc[:,np.where([p in list_features for p in calibdata.columns])[0]]
    elif config.selection is Selection.Automatic:
        print("Executing Automatic selection ...")
        calib1, calib2 = train_test_split(calibdata, test_size=0.7, random_state=0)
        
        print("Training SequentialFeatureSelector...")
        sfs = SFS(config.algo, k_features=config.no_of_selection_features, forward=True, floating=True, scoring='r2', cv=5)
        sfs.fit(calib1.iloc[:,1:], calib1.iloc[:,0])
        list_features= ['yield', *list(sfs.get_metric_dict()[np.argmax([sfs.get_metric_dict()[i]['avg_score'] for i in range(1, config.no_of_selection_features+1)])+1]['feature_names'])]
        print('R² Calibration : ' + str(np.max([sfs.get_metric_dict()[i]['avg_score'] for i in range(1,config.no_of_selection_features+1)])))
        calibdata = calib2.iloc[:, np.where([p in list_features for p in calib2.columns])[0]]
        print("Calibration data 2 = {}".format(calibdata))
    else:
        print("Using all features (no manual or automatic mode)...")
        list_features = calibdata.columns

    # Model training
    print("Cleaning calibration data ...")
    clean_dataset(calibdata)
    
    # print("==============================================")
    # print(calibdata.to_string())
    # print("==============================================")
    # print("calibdata.iloc[:,1:]")
    # print(calibdata.iloc[:,1:].to_string())
    # print("==============================================")
    # print("calibdata.iloc[:,0]")
    # print(calibdata.iloc[:,0].to_string())
    # print("==============================================")
    
    print("Training model with {} ...".format(config.algo))
    config.algo.fit(calibdata.iloc[:,1:], calibdata.iloc[:,0])
    
    return list_features

def apply_model(algo, merged_features, list_features):

    # Model Apply
    # print("Algo 1: ")
    # print(algo)
    # print("Merged features : ")
    # print(merged_features.to_string())
    # print("List features :")
    # print(list_features)
    fieldestim = pd.DataFrame({"NewID": merged_features["NewID"], "Estimation": algo.predict(merged_features.iloc[:, np.where([p in list_features[1:] for p in merged_features.columns])[0]])})
    return fieldestim

def aggregate_at_statistical_unit(fieldestim, statistical_unit_fields_file):
    # read the file providing the mapping from the fields to statistical units
    statistical_unit_fields = pd.read_csv(config.statistical_unit_fields_file, names=['NewID','SU','AreaField'])  #Table including Stat. Unit ID by field, Area of each field
    # Agregation at Statistical Unit
    SUestim = pd.merge(statistical_unit_fields,fieldestim,on='NewID')
    SUestim['Production']=SUestim['AreaField']*SUestim['Estimation']
    SUestim= SUestim.groupby('SU').sum().reset_index()
    SUestim['Estimation']=SUestim['Production']/SUestim['AreaField']

    return SUestim

def main():
    parser = argparse.ArgumentParser(
        description="Yield model computation"
    )
    parser.add_argument(
        "-a", "--algo", required=False, default="rf", help="The algorithm to be used. lm - LinerarRegression, svm - SupportVectortMachine. Default rf = RandomForest", choices=['rf', 'lm', 'svm']
    )
    parser.add_argument(
        "-s", "--selection", required=False, default="none", help="The selection mode. Possible values: automatic or manual or none", choices=['none', 'manual', 'automatic'] 
    )

    parser.add_argument(
        "-m", "--manual-selection-features", required=False, help="The selection features list for the manual mode", nargs='+', type=str
    )

    parser.add_argument(
        "-n", "--max-automatic-features-no", required=False, help="The maximum number of selection features for the automatic mode", type=int, default = 44
    )
    
    parser.add_argument(
        "-i", "--input-features", required=True, help="The input features file"
    )

    parser.add_argument(
        "-r", "--yield-reference", required=True, help="The input yield reference file"
    )

    parser.add_argument(
        "-c", "--crop-codes", required=True, help="List of crop codes"
    )  ### change  ex : -c /mnt/archive/orchestrator_temp/s4s_yield_feat/8067/81836-s4s-merge-lai-with-grid/lai_with_grid.csv -> I saw that the crop type was added in the lai with grid file

    parser.add_argument(
        "-u", "--statistical-unit-fields", required=False, help="The input statistical unit fields mapping file"
    )
    
    parser.add_argument(
        "-o", "--output", required=True, help="The output estimation file"
    )

    parser.add_argument(
        "-e", "--output-statistical-units-estimate", help="The output for statistical units estimation file"
    )
    parser.add_argument('--has-trend', default=False, action='store_true')
    
    args = parser.parse_args()
    config = Config(args)

    if args.statistical_unit_fields is not None:
        if args.output_statistical_units_estimate is None:
            print("Statistical units estimate output file is not provided but is required as --statistical-unit-fields was given. Exiting ...")
            sys.exit(1)
            
    # Read the input files 
    print("Reading input features from {}".format(config.input_features))
    in_feat_names = INPUT_FEATURE_NAMES.copy()
    
    columns_to_ignore = []
    yield_ref_columns = []
    if args.has_trend:
        # remove SAFY columns
        columns_to_ignore = ["Yield", "d0out", "SenBout"]
        yield_ref_columns = in_feat_names.copy()
        yield_ref_columns.insert(len(yield_ref_columns)-1, "year")
    else :
        # remove Trend column
        columns_to_ignore = ["Trend"]
        yield_ref_columns = ['NewID','yield_estimate']
        
    merged_features = pd.read_csv(config.input_features, sep=',', names=in_feat_names, header = 1)    
    # merged_features['ColdT0'] = merged_features['ColdT0'].astype(float)
    # print(merged_features)
    crop_codes = pd.read_csv(config.crop_codes, sep=',')[['crop_code']] ### change
    # merged_features = merged_features.merge(id2crop, on='NewID')   ### change

    print("Reading yield reference from {}".format(config.yield_reference))
    print("Yield referece columns are {}".format(yield_ref_columns))
    yield_ref = pd.read_csv(config.yield_reference, sep=',', names=yield_ref_columns, header = 1)

    print("Cleaning input features for NaN")
    merged_features = remove_ignoring_columns(merged_features, columns_to_ignore)
    yield_ref = remove_ignoring_columns(yield_ref, columns_to_ignore)
    
    # print(merged_features)
    # print(yield_ref)
    
    clean_dataset(merged_features)
    clean_dataset(yield_ref)
    # print(merged_features)
    
    fieldestim= pd.DataFrame(columns=["NewID", "Estimation",'crop_code'])
    if args.statistical_unit_fields is not None:
        SUestim = pd.DataFrame(columns=['SU','Estimation','crop_code'])

    for cc in np.unique(crop_codes['crop_code']): ### change
        merged_features_cc =  merged_features[merged_features['crop_code'] == cc] ### change
        # print("Crop Code {} Values: {}".format(cc, merged_features_cc))

        if len(merged_features_cc)<5 : ### change
            # print("Too few features provided {}".format(len(merged_features_cc)))
            continue  ### change

        # train the model
        if args.has_trend:
            # remove the NewID column
            calibdata = yield_ref.iloc[:,1:]
            # remove the crop_code column
            calibdata = calibdata.iloc[:,:-1]
            # move the Trend column as the first column
            col = calibdata.pop("Trend")
            calibdata.insert(0, col.name, col)
        else :
            feats = merged_features_cc.iloc[:,:-1]
            calibdata = pd.merge(yield_ref, feats,on='NewID').iloc[:,1:]
            
        # print("=====================================================")
        # print("Calibration data : {}".format(str(calibdata))) ### change
        # print("=====================================================")
        
        print("Training model {} ...".format(str(cc))) ### change
        list_features = train_model(config, calibdata) ### change
        
        # print(list_features)
        
        # apply the model
        print("Applying model {}...".format(str(cc))) ### change
        
        data_features = merged_features_cc.copy()
        if args.has_trend:
            # print("Orig :")
            # print("=====================================================")
            # print(data_features.to_string())
            
            data_features.insert(len(data_features.columns)-1, 'year', '2020')
            
            # print("With year column:")
            # print("=====================================================")
            # print(data_features.to_string())
            # print("=====================================================")
        
        fieldestimcc = apply_model(config.algo, data_features.iloc[:,:-1], list_features) ### change
        fieldestimcc['crop_code'] = cc  ### change
        
        # print(fieldestimcc)
        
        fieldestim = pd.concat([fieldestim,fieldestimcc]) ### change

        if args.statistical_unit_fields is not None:
            # Perform aggregation at statistical units level
            print("Performing aggregation at statistical unit level {} ...".format(str(cc)))
            SUestimcc = aggregate_at_statistical_unit(fieldestim, config.statistical_unit_fields)[['SU','Estimation']] ### change
            SUestimcc['crop_code'] = cc  ### change
            SUestim = pd.concat([SUestim,SUestimcc])  ### change

    # write the output files
    print("Writing output file {}".format(args.output))
    fieldestim.to_csv(args.output,index=False)

    if args.statistical_unit_fields is not None:
        print("Writing statistical units estimate into{}".format(args.output_statistical_units_estimate))
        SUestim.to_csv(args.output_statistical_units_estimate, index=False)  ### change

if __name__ == "__main__":
    main()
    