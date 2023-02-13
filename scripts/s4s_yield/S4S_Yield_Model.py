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
            
def train_model(config, yield_ref, merged_features) : 
    calibdata = pd.merge(yield_ref, merged_features,on='NewID').iloc[:,1:]
    print("Calibration data (initial) = {}".format(calibdata))

    if config.selection is Selection.Manual:
        print("Executing manual selection ...")
        list_features = config.manual_selection_features_list
        calibdata = calibdata.iloc[:,np.where([p in list_features for p in calibdata.columns])[0]]
    elif config.selection is Selection.Automatic:
        print("Executing Automatic selection ...")
        calib1, calib2 = train_test_split(calibdata, test_size=0.7, random_state=0)
        sfs = SFS(config.algo, k_features=config.no_of_selection_features, forward=True, floating=True, scoring='r2', cv=5)
        sfs.fit(calib1.iloc[:,1:], calib1.iloc[:,0])
        list_features= ['yield', *list(sfs.get_metric_dict()[np.argmax([sfs.get_metric_dict()[i]['avg_score'] for i in range(1, config.no_of_selection_features+1)])+1]['feature_names'])]
        print('R² Calibration : ' + str(np.max([sfs.get_metric_dict()[i]['avg_score'] for i in range(1,config.no_of_selection_features+1)])))
        calibdata = calib2.iloc[:, np.where([p in list_features for p in calib2.columns])[0]]
        print("Calibration data 2 = {}".format(calibdata))
    else:
        list_features = calibdata.columns

    # Model training
    config.algo.fit(calibdata.iloc[:,1:], calibdata.iloc[:,0])
    
    return list_features

def apply_model(algo, merged_features, list_features):
    # Model Apply
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
        "-u", "--statistical-unit-fields", required=False, help="The input statistical unit fields mapping file"
    )
    
    parser.add_argument(
        "-o", "--output", required=True, help="The output estimation file"
    )

    parser.add_argument(
        "-e", "--output-statistical-units-estimate", required=True, help="The output for statistical units estimation file"
    )
    
    args = parser.parse_args()
    config = Config(args)

    # Read the input files 
    merged_features = pd.read_csv(config.input_features, sep=',', names=['NewID','ColdT0','ColdT1','HotT2','SumT1','SumT2','SumT251','SumT252','SumP1','SumP2','SumR1','SumR2','SumE1','SumE2','MeanT1','MeanT2','MeanP1','MeanP2','MeanR1','MeanR2','MeanE1','MeanE2','MeanSW10','MeanSW11','MeanSW12','MeanSW20','MeanSW21','MeanSW22','MeanSW30','MeanSW31','MeanSW32','MeanSW40','MeanSW41','MeanSW42','Yield','d0out','SenBout'])    # TODO: add the other feature names here
    print(merged_features)
    yield_ref = pd.read_csv(config.yield_reference, sep=',', names=['NewID','yield_estimate'])
    print(yield_ref)
    
    # train the model
    list_features = train_model(config, yield_ref, merged_features)
    
    # apply the model
    fieldestim = apply_model(config.algo, merged_features, list_features)

    # write the output files
    fieldestim.to_csv(args.output,index=False)

    if args.statistical_unit_fields is not None:
        # Perform aggregation at statistical units level
        SUestim = aggregate_at_statistical_unit(fieldestim, config.statistical_unit_fields)
        SUestim[['SU','Estimation']].to_csv(args.output_statistical_units_estimate, index=False)


if __name__ == "__main__":
    main()
