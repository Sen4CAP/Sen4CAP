import psycopg2
import os,optparse,sys
from glob import glob
import subprocess
import os
import xmltodict
import json,csv
from numpy.matlib import repmat
from pandas import isnull as isnull
import numpy as np
import datetime as dt

####### Intro SCRIPT ############################

#DirData = os.environ['DirDataYield']
DirData = '/mnt/output/Sen4Stat/Scripts_Test_CSRO/Yield/'

class OptionParser (optparse.OptionParser):
    def check_required (self, opt):
      option = self.get_option(opt)
      # Assumes the option's 'default' is set to None!
      if getattr(self.values, option.dest) is None:
          self.error("%s option not supplied" % option)

def PrintHelp(argv,ConfigArgs=[]):
    if len(argv) == 1:
        prog = os.path.basename(argv[0])
        print('      '+argv[0]+' [options]')
        print("     Aide : "+prog+" --help")
        print("        ou : "+prog+" -h")

        print("exemple : ")
        print('python '+argv[0]+' -c CountryShort ')
        sys.exit(-1)
    else:
        usage = "usage: %prog [options] "
        parser = OptionParser(usage=usage)
        parser.add_option("-c", "--country", dest="CountryShort", action="store", \
            help="CountryShort to process", type="string", default='BE')
        if 'y' in ConfigArgs:
            parser.add_option("-y", "--year", dest="YearStr", action="store", \
                help="Year to process [eg, 2015, only one]", type="string", default='9999')
        (options, args) = parser.parse_args()
    return options

def GetECYFSpar(CountryShort,YearStr='9999'):
    #DirECYFS = '/export/miro/claveriem/Code/ECYFS/'
    #sys.path.append(DirECYFS)
    Epar = Import_ECYFS_Parameters(CountryShort,YearStr=YearStr)

    ScheMa = Epar['country'].replace(' ', '')
    # EPSGParc = Epar['EPSGParc']
    # NameParc = Epar['NameParc']
    # Weathersource = Epar['Weathersource']
    # Continent = Epar['Continent']
    # if Continent == 'USA':
    #     StateCode = Epar['StateCode']
    # else:
    #     StateCode = ''
    # YearStart = int(Epar['YearStart'])
    # YearEnd   = int(Epar['YearEnd'  ])
    # yearL = Epar['Ylist'] # list(range(int(Epar['YearStart']), int(Epar['YearEnd'])+1))

    # if YearStr=='9999':
    #     Year2Pro = range(int(Epar['YearStart']),int(Epar['YearEnd'])+1)
    # else:
    #     Year2Pro = [int(YearStr)]

    return Epar

        
def Import_ECYFS_Parameters(CountryShort,YearStr='9999'):
    fname = DirData+'Parameters/'+CountryShort+'.json'
    # if os.path.isfile(fname) and YearStr=='9999' :
    #     f = open(fname, 'r')
    #     ECYFS_Parameters = json.load(f)
    #     f.close()
    # else:
    with open(DirData+'Parameters/Yield_Parameters.xml') as fd:
        BigDict = dict(xmltodict.parse(fd.read()))    
    CountryPar = BigDict['Yield']['countries'][CountryShort]
    ECYFS_Parameters = BigDict['Yield']['generic']
    for k in CountryPar.keys():
        ECYFS_Parameters[k] = CountryPar[k]
    if YearStr=='9999':
        YearStart = int(ECYFS_Parameters['YearStart'])
        YearEnd = int(ECYFS_Parameters['YearEnd'])    
        Ylist = list(range(YearStart,YearEnd+1))
    else:
        Ylist = [int(YearStr)]
    ECYFS_Parameters['Ylist']=Ylist
    f = open(fname,'w')
    json.dump(ECYFS_Parameters,f)
    f.close()
    return ECYFS_Parameters
                

####### ConnectPG ############################

HostIP='127.0.0.1'
ScheMa='yield'
def LaunchPG(Commande):
    print ("Executing command: {}".format(Commande))
    # (conn,cur)=InitPG()
    # conn = psycopg2.connect(\
    #     "dbname='sen4stat' user='admin' host='"+HostIP+"' password='sen2agri'")
    # cur = conn.cursor()
    # cur.execute(Commande)
    # cur.close()
    # conn.commit()
    # conn.close()

def InitPG():
    conn = psycopg2.connect(\
        "dbname='sen4stat' user='admin' host='"+HostIP+"' password='sen2agri'")
    cur = conn.cursor()
    return conn,cur

def FinishPG(conn,cur):
    cur.close()
    conn.commit()
    conn.close()

def LaunchPGwithoutput(Commande):
    print ("Executing command: {}".format(Commande))
    (conn,cur)=InitPG()
    cur.execute(Commande)
    out=cur.fetchall()
    FinishPG(conn,cur)
    PGok = 1
    # print ("Executed command returned output: {}".format(out))
    return out

def PgId():
    return u"'host=" + HostIP + " port=5432 user=s4s_sirs dbname=sen4stat password=OPpds66BSJIAgM0k'"

def PGrun(SQLin):
    print ("Executing command: {}".format(SQLin))
#     conn = psycopg2.connect(\
#         "dbname='sen4stat' user='s4s_sirs' host='"+HostIP+"' password='OPpds66BSJIAgM0k'")
#         # "dbname='martin_db' user='martin' host='"+HostIP+"' password='y!G??..va2fx*q%'")
#     cur = conn.cursor()
#     # try:
#     cur.execute('SET search_path TO '+ScheMa+',public;'+SQLin)
#     # except:
#     #     print('Command '+SQLin[0:5]+'... not run')
#     cur.close()
#     conn.commit()
#     conn.close()

####### Generic functions ############################

def MakeNewDir(Path):
    if not os.path.exists(Path):
        os.makedirs(Path)


def RunSysCommand(Commande):
    p = subprocess.Popen(Commande, stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    return output

def RepmatText(IN,N):
    return ''.join(repmat(IN,1,N)[0])

def NoneInt(x):
    if x!=None:
        x=int(x)
    return x
def NoneFloat(x):
    if x!=None:
        x=float(x)
    return x
def SubdivideDataset(N,k):
    Nd = int(N/k)
    if Nd==0:
        S=[0]
        E=[N]
        return S,E
    cl = range(1,Nd+1)
    S=[] ; E=[]
    for i in cl:
        S.append(int((i-1)*k+1))
        E.append(int(i*k))
    if E[-1]<N:
        E[-1]=N
    return S,E

def NoneAdd(IN,n):
    if IN==None:
        OUT=IN  
    else:
        OUT=IN+n 
    return OUT

def NoneAbs(IN,n):
    if IN==None:
        OUT=IN  
    else:
        OUT=IN+n 
    return OUT

def rmse(x,y):
    t = ~isnull(x+y)
#     import pdb; pdb.set_trace()
    x=x[t] ; y=y[t]
    return np.sqrt(np.sum(np.power(x - y,2))/np.sum(t))

