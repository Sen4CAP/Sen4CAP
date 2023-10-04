#!/usr/bin/env python

import argparse
import datetime
from datetime import date, datetime, time, timedelta
import pandas as pd
import numpy as np

niv_Conf = [np.nan,'nan','Doubtful','Poor','Medium','Good','Strong','Strong+']

class Config(object):
    def __init__(self, args):

        self.year = args.year

        self.markers_start_date = args.start_date
        self.markers_end_date = args.end_date

        self.p_short = timedelta(days=args.p_short)
        self.p_long = timedelta(days=args.p_long)

        self.s2_periods = args.s2_periods
        self.s1_periods = args.s1_periods

        self.thr_bs_s2 = args.thr_bs_s2
        self.thr_nbs_s2 = args.thr_nbs_s2
        self.thr_bs_s1 = args.thr_bs_s1
        self.thr_nbs_s1 = args.thr_nbs_s1

def GetPeriodsBS(S_i,Pshort,Plong,thr_bs,thr_nbs,year):
    #Analyse S2 - START period
    START_BS = 'nan'
    END_BS = 'nan'
    Conf_BS = np.nan
    M1 = 0
    M2 = np.nan
    M3 = np.nan
    M4 = np.nan
    M5 = np.nan
    M6 = np.nan
    NbrBS = np.nan
    NbrTT = 0
    EndLook = False
    LookNext = False
    ind = S_i.index
    M3_transition = 0
    baresoil_lastdate = 'nan'

    for idx in ind:
        NbrTT += 1 
        # look for START_BS

        if (np.isnan(M1)| (M1==0)) & (S_i.at[idx,'pred']=='BS') & (S_i.at[idx,'conf']>=thr_bs):
            M1 = 1
            START_BS = S_i.at[idx,'dates']
            NbrBS = 0
            d_start = datetime.strptime(START_BS,'%Y-%m-%d')
            continue
        elif (M1 != 1):
            d_start = datetime.strptime(f'{year}-01-01','%Y-%m-%d')

        d_continue = datetime.strptime(S_i.at[idx,'dates'],'%Y-%m-%d')
        
        if (M1 == 1) & (d_continue-d_start > Pshort) & (END_BS=='nan') & (M2 == 0):
            END_BS = START_BS
            EndLook = False
            LookNext = True
        elif (M1 == 1) & (d_continue-d_start > Plong) & (END_BS=='nan') & (M4==0) & (baresoil_lastdate != 'nan'):
            END_BS = baresoil_lastdate
            EndLook = False
            LookNext = True

        elif (M1==1) and (END_BS=='nan'):
            NbrBS += 1
            if np.isnan(M2):
                M2 = 0
                M3 = 0
            if (S_i.at[idx,'pred']=='BS'):
                EndLook = True
                M4 = 0
                baresoil_lastdate = 'nan'
                M3_transition = 0

            if (S_i.at[idx,'conf'] > thr_bs) & (S_i.at[idx,'pred']=='BS') :
                M2 += 1
                M3 += 2
            
            elif (S_i.at[idx,'pred']=='BS') & (S_i.at[idx,'conf'] <= thr_bs) :
                M3 += 1

            elif ((S_i.at[idx,'pred']!='BS') & (S_i.at[idx,'conf'] <= thr_nbs)): 
                M3 += -2
                M3_transition += 2
                if baresoil_lastdate=='nan':
                    baresoil_lastdate = S_i.at[idx-1,'dates']
                       

        # look for the END_BS

        if (M1==1) & (EndLook) :
            LookNext = True
            if (M4==0) & (S_i.at[idx,'pred']!='BS') & (S_i.at[idx,'conf']> thr_nbs):
                M4 = 1
                if baresoil_lastdate=='nan':
                    END_BS = S_i.at[idx-1,'dates']
                    
                else:
                    END_BS = baresoil_lastdate
                    M3 += M3_transition
                    NbrBS += - M3_transition/2

                d_end = datetime.strptime(END_BS,'%Y-%m-%d')
                    
                continue
            elif (M4!=1):
                M4 = 0
                d_end = d_start

            if (M4==1) & (d_continue-d_end < Plong):
                if np.isnan(M5):
                    M5 = 0
                    M6 = 0
                if (S_i.at[idx,'conf']>thr_nbs) & (S_i.at[idx,'pred']!='BS') :
                    M5 += 1
                    M6 += 2
                elif (S_i.at[idx,'conf']<= thr_nbs) & (S_i.at[idx,'pred']!='BS'):
                    M6 += 1
                elif (S_i.at[idx,'pred']=='BS') & (S_i.at[idx,'conf']>thr_bs) & (d_continue-d_end < Pshort):
                    M6 += -2
                    #print('end_next')
                    if M6 <= -4 :
                        M4 = 0
                        END_BS = 'nan'
                        continue
                elif (S_i.at[idx,'pred']=='BS'):
                    M6 += -2
                else : 
                    M6 += 0

        #print(d_continue)
        #print(START_BS)
        #print('M2: ' + str(M2))
        #print('M3: ' + str(M3))
        #print('M3_t: ' + str(M3_transition))
        #print(END_BS)
        #print(M6)
        #print(baresoil_lastdate)

    if ((M2 >= 1) & (M4 == 0) & (baresoil_lastdate=='nan')):
        END_BS = 'Continue'
        LookNext = False
    elif (baresoil_lastdate!='nan'):
        END_BS = baresoil_lastdate

    if (END_BS=='nan'):
        Conf_BS = 'nan'
# according to the value of M1-M3 --> assess the confidence start and the start_date
 
    if (M2 == 0) & (np.isnan(M4) | (M4 == 0)) :
        END_BS = START_BS

    if (M1 == 1) & (M2 >= 3) & (M3 >= 2) & (M6 > 0):
        Conf_BS = 'Strong'
    
    elif (M1 == 1) & (M2 > 0) & (M3 >= 0) & (M5>0):
        Conf_BS = 'Good'
    
    elif (M1 == 1) & (M2 > 0) & (M3 >= 0):
        Conf_BS = 'Medium'

    elif ((M1==1) & (M2 == 0) & (M3 >= 0)) | ((M2 >= 0) & (M3 <=0)):
        Conf_BS = 'Poor'

    if (M1==1) & (M3 < 0) & (END_BS == START_BS):
        Conf_BS = 'Doubtful'
        
    

# set the END date if M1 not confirm and END date note found 
        
    return(M1,M2,M3,M4,M5,M6,START_BS,END_BS,Conf_BS,NbrBS,LookNext,NbrTT)


def OutputMarkers(Sat,NPeriod,df_results,i,Pshort,Plong,thr_bs,thr_nbs,year):
    for p in range(1,NPeriod+1):
        if p == 1:
            S2_i = df_results.loc[df_results.NewID==i]
            markers = list(GetPeriodsBS(S_i=S2_i,Pshort=Pshort,Plong=Plong,thr_bs=thr_bs,thr_nbs=thr_nbs, year=year))
            NbrTT = markers[11]
            LastObs = lambda x: np.nan if x.empty else x.iloc[-1]['dates']
            df_out1 = {
            'NewID' : i,
            f'NbrTT{Sat}' : NbrTT,
            f'LastObs{Sat}': LastObs(S2_i),
            f'M1_{Sat}_{p}' : markers[0],
            f'M2_{Sat}_{p}' : markers[1],
            f'M3_{Sat}_{p}' : markers[2],
            f'M4_{Sat}_{p}' : markers[3],
            f'M5_{Sat}_{p}' : markers[4],
            f'M6_{Sat}_{p}' : markers[5],
            f'STARTBS_{Sat}_{p}' : markers[6],
            f'ENDBS_{Sat}_{p}' : markers[7],
            f'Conf_{Sat}_{p}' : markers[8],
            f'Nbr{Sat}BS_{p}' : markers[9],
            f'Look_next{Sat}_{p}' : markers[10]
            }

        else :
            if markers[10]:
                S2_i2 = df_results.loc[(df_results.NewID==i) & (df_results.date_d > (datetime.strptime(markers[7],'%Y-%m-%d') + Pshort))]
                markers = list(GetPeriodsBS(S2_i2,Pshort,Plong,thr_bs,thr_nbs,year))
            else:
                S2_i2 = df_results.head(0)
                markers = list(GetPeriodsBS(S2_i2,Pshort,Plong,thr_bs,thr_nbs,year))

            df_out1.setdefault(f'M1_{Sat}_{p}',markers[0])
            df_out1.setdefault(f'M2_{Sat}_{p}',markers[1])
            df_out1.setdefault(f'M3_{Sat}_{p}',markers[2])
            df_out1.setdefault(f'M4_{Sat}_{p}',markers[3])
            df_out1.setdefault(f'M5_{Sat}_{p}',markers[4])
            df_out1.setdefault(f'M6_{Sat}_{p}',markers[5])
            df_out1.setdefault(f'STARTBS_{Sat}_{p}',markers[6])
            df_out1.setdefault(f'ENDBS_{Sat}_{p}',markers[7])
            df_out1.setdefault(f'Conf_{Sat}_{p}',markers[8])
            df_out1.setdefault(f'Nbr{Sat}BS_{p}',markers[9])
            df_out1.setdefault(f'Look_next{Sat}_{p}',markers[10])

    
    return(df_out1)
    

def S1_S2_comparaison_and_countdays(i,dti_S2,dti_S1,NPeriodS2,NPeriodS1):
    tt_daysS2=np.nan
    tt_daysS1=np.nan
    tt_daysS1S2 = np.nan
    dti_S2 = dti_S2.reset_index()
    dti_S1 = dti_S1.reset_index()
    ending = lambda x,s,t: datetime.strptime(x.loc[0,f'LastObs{s}'],'%Y-%m-%d') if x.loc[0,f'ENDBS_{s}_{t}'] in ('Continue','nan') else datetime.strptime(x.loc[0,f'ENDBS_{s}_{t}'],'%Y-%m-%d')
    delta_zero = lambda x: 0 if x < 0 else x
    periodsS2_out = []
    periodsS1_out = []
    
    if NPeriodS2!= 0 :
        tt_daysS2 = 0
        NbrS2TT = dti_S2.loc[0,'NbrTTS2']
        LastObsS2 = dti_S2.loc[0,'LastObsS2']
        if NPeriodS1 == 0:

            for p in range(1,NPeriodS2+1):
                periodsS2_out.append(dti_S2.loc[0,f'STARTBS_S2_{p}'])
                periodsS2_out.append(dti_S2.loc[0,f'ENDBS_S2_{p}'])
                periodsS2_out.append(dti_S1.loc[0,f'Conf_S2_{p1}'])
                if dti_S2.loc[0,f'M1_S2_{p}'] == 1:
                    deltaS2 =  ending(dti_S2,'S2',p) - datetime.strptime(dti_S2.loc[0,f'STARTBS_S2_{p}'],'%Y-%m-%d')
                    if deltaS2.days == 0:
                        tt_daysS2 += 1
                    else:
                        tt_daysS2 += deltaS2.days
        
        else:
            tt_daysS1S2 = 0
            for p in range(1,NPeriodS2+1):
                periodsS2_out.append(dti_S2.loc[0,f'STARTBS_S2_{p}'])
                periodsS2_out.append(dti_S2.loc[0,f'ENDBS_S2_{p}'])
                Conf_S2 = dti_S2.loc[0,f'Conf_S2_{p}']
                if dti_S2.loc[0,f'M1_S2_{p}'] == 1:
                        deltaS2 =  ending(dti_S2,'S2',p) - datetime.strptime(dti_S2.loc[0,f'STARTBS_S2_{p}'],'%Y-%m-%d')
                        if deltaS2.days == 0:
                            tt_daysS2 += 1
                        else:
                            tt_daysS2 += deltaS2.days
                for p1 in range(1,NPeriodS1+1):
                    if (dti_S2.loc[0,f'M1_S2_{p}'] == 1) & (dti_S1.loc[0,f'M1_S1_{p1}'] == 1):
                        startS2 = datetime.strptime(dti_S2.loc[0,f'STARTBS_S2_{p}'],'%Y-%m-%d')
                        startS1 = datetime.strptime(dti_S1.loc[0,f'STARTBS_S1_{p1}'],'%Y-%m-%d')
                        start = max(startS2,startS1)
                        end = min(ending(dti_S2,'S2',p),ending(dti_S1,'S1',p1))
                        delta =  end - start
                        #print(delta)
                        tt_daysS1S2 += delta_zero(delta.days)
                        #print('tt_daysS1S2 : ',tt_daysS1S2)
                        if delta.days > 0:
                            Conf_S2 = niv_Conf[niv_Conf.index(dti_S2.loc[0,f'Conf_S2_{p}'])+1]+'_S1'
                periodsS2_out.append(Conf_S2)
                
    if NPeriodS1!= 0 :
        tt_daysS1 = 0
        NbrS1TT = dti_S1.loc[0,'NbrTTS1']
        LastObsS1 = dti_S1.loc[0,'LastObsS1']
        for p1 in range(1,NPeriodS1+1):
            periodsS1_out.append(dti_S1.loc[0,f'STARTBS_S1_{p1}'])
            periodsS1_out.append(dti_S1.loc[0,f'ENDBS_S1_{p1}'])
            periodsS1_out.append(dti_S1.loc[0,f'Conf_S1_{p1}'])
            if dti_S1.loc[0,f'M1_S1_{p1}'] == 1:
                deltaS1 =  ending(dti_S1,'S1',p1) - datetime.strptime(dti_S1.loc[0,f'STARTBS_S1_{p1}'],'%Y-%m-%d')
                #print(delta)
                if deltaS1.days == 0:
                    tt_daysS1 += 1
                else:
                    tt_daysS1 += deltaS1.days

    
    return(i,*periodsS2_out,NbrS2TT,LastObsS2,tt_daysS2,tt_daysS1S2,*periodsS1_out,NbrS1TT,LastObsS1,tt_daysS1)


def extract_markers(cfg, fileS2, fileS1, file_outS2, file_outS1, file_outTT) :
    # # Sentinel-2 & Sentinel-1
    df_resultsS2 = pd.read_csv(fileS2)
    df_resultsS2['date_d'] = pd.to_datetime(df_resultsS2['dates'])
    df_resultsS2 = df_resultsS2.loc[(df_resultsS2.date_d >= datetime.strptime(cfg.markers_start_date,'%Y-%m-%d')) & 
                                    (df_resultsS2.date_d <= datetime.strptime(cfg.markers_end_date,'%Y-%m-%d')) ]
    df_resultsS1 = pd.read_csv(fileS1)
    df_resultsS1['date_d'] = pd.to_datetime(df_resultsS1['dates'])
    df_resultsS1 = df_resultsS1.loc[(df_resultsS1.date_d >= datetime.strptime(cfg.markers_start_date,'%Y-%m-%d')) & 
                                    (df_resultsS1.date_d <= datetime.strptime(cfg.markers_end_date,'%Y-%m-%d')) ]

    list_id = np.unique(df_resultsS2['NewID'])

    print('All imported')
    dfS2_out = []
    #Look_two_p = True

    if cfg.s2_periods != 0 :
        for i in list_id:
            #Sentinel-2
            df_out1 = OutputMarkers('S2', cfg.s2_periods, df_resultsS2, i, Pshort = cfg.p_short, Plong = cfg.p_long,
                                    thr_bs = cfg.thr_bs_s2, thr_nbs = cfg.thr_nbs_s2, year = cfg.year)
            dfS2_out.append(df_out1)

        dt_S2 = pd.DataFrame(dfS2_out)
        dt_S2.to_csv(file_outS2, index = False)

    dfS1_out = []

    if cfg.s1_periods != 0 :
        for i in list_id:
            #Sentinel-1
            df_out1 = OutputMarkers('S1', cfg.s1_periods, df_resultsS1, i, Pshort=cfg.p_short, Plong = cfg.p_long, 
                                    thr_bs = cfg.thr_bs_s1, thr_nbs = cfg.thr_nbs_s1, year = cfg.year)
            dfS1_out.append(df_out1)

        dt_S1 = pd.DataFrame(dfS1_out)
        dt_S1.to_csv(file_outS1, index=False)

    outputfinal = []

    list_outputname = ['NewID']
    list_outputname.extend([name for p in range(1, cfg.s2_periods+1) if cfg.s2_periods != 0 for name in [f'START_S2_{p}', f'END_S2_{p}', f'Conf_S2_{p}']])
    list_outputname.extend(['NbrTTS2', 'LastObsS2', 'TTdaysS2', 'TTdaysS1S2'])
    list_outputname.extend([name for p in range(1, cfg.s1_periods+1) if cfg.s1_periods != 0 for name in [f'START_S1_{p}', f'END_S1_{p}', f'Conf_S1_{p}']])
    list_outputname.extend(['NbrTTS1', 'LastObsS1', 'TTdaysS1'])
    print(list_outputname)

    for i in list_id:

        dti_S2 = dt_S2.loc[dt_S2['NewID']==i,:]

        if dt_S1.empty:
            dti_S1 = np.nan
        else :
            dti_S1 = dt_S1.loc[dt_S1['NewID']==i,:]

        output = S1_S2_comparaison_and_countdays(i, dti_S2, dti_S1, cfg.s2_periods, cfg.s1_periods)
        outputfinal.append(output)

    Outputfinal = pd.DataFrame(outputfinal)
    Outputfinal.columns = list_outputname

    Outputfinal.to_csv(file_outTT,index=False)  

def main():
    parser = argparse.ArgumentParser(
        description="Performs bare soil markers extraction"
    )
    parser.add_argument("-i", "--input-s2", help="Input S2 results file", required=True)
    parser.add_argument("-j", "--input-s1", help="Input S1 results file", required=True)
    parser.add_argument("-y", "--year", help="The processing year", required=True, type=int)
    parser.add_argument("-m", "--out-markers-s2", help="Output S2 markers", required=True)
    parser.add_argument("-n", "--out-markers-s1", help="Output S1 markers", required=True)
    parser.add_argument("-o", "--out-markers-all", help="All markers output", required=True)
    
    parser.add_argument("-s", "--start-date", help="Markers period start date", required=True)
    parser.add_argument("-e", "--end-date", help="Markers period end date", required=True)

    parser.add_argument("--p-long", help="""Standing for Long Period. It is used as the duration in days
                                            to look for vegetation (NBS) after the end of the bare soil period (END_BS)""",
                        required=False, type=int, default=60)
    parser.add_argument("--p-short", help="""The short period. Is the maximum number of days after after the start of the 
                                                bare soil period (START_BS) where if the END_BS is not found, the ENDS_BS is equal 
                                                to the START_BS.""", required=False, type=int, default=30)

    parser.add_argument("--s2-periods", help="Number of S2 periods", required=False, type=int, default=3)
    parser.add_argument("--s1-periods", help="Number of S1 periods", required=False, type=int, default=4)

    parser.add_argument("--thr-bs-s2", help="Threshold BS S2", required=False, type=float, default=0.75)
    parser.add_argument("--thr-nbs-s2", help="Threshold NBS S2", required=False, type=float, default=0.8)
    parser.add_argument("--thr-bs-s1", help="Threshold BS S1", required=False, type=float, default=0.65)
    parser.add_argument("--thr-nbs-s1", help="Threshold NBS S1", required=False, type=float, default=0.7)
    
    args = parser.parse_args()

    config = Config(args)

    # Extract Markers
    extract_markers(config, args.input_s2, args.input_s1, args.out_markers_s2, args.out_markers_s1, args.out_markers_all)

if __name__ == "__main__":
    main()







