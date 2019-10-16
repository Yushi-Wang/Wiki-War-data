# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 09:06:30 2019

@author: leo
"""


import numpy as np
import pandas as pd
import sys
import re
import random
import datetime
import datetime
import calendar
from os import mkdir
import math
main_path = "D:/learning/Arash/war_participants/Wiki-War Data/"
try:
    mkdir(main_path)
except FileExistsError:
    pass
now = datetime.datetime.now()
time=str(now)
time=time[0:10]
missingtable_path=main_path+'tem/WWdata_descriptive_table_infobox'+time+'.xls'
participant_path="D:/learning/Arash/war_participants/Fabian_06292019_WAR/Data/participant.tsv"
qid_time_location_path='D:/learning/Arash/war_participants/Fabian_06292019_WAR/Data/qid_time_location.csv'
war_participant_infobox_path="D:/learning/Arash/war_participants/articles/infobox/output/war_participant_infobox.csv"
war_commander_infobox_path="D:/learning/Arash/war_participants/articles/infobox/output/wikiWAR_commander_infobox.csv"
war_partof_infobox_path="D:/learning/Arash/war_participants/articles/infobox/output/partof_infobox.csv"
wid_to_qid_path='D:/learning/Arash/war_participants/articles/infobox/input/wid_to_qid.tsv'
qid_battle_path="D:/learning/Arash/war_participants/Fabian_06292019_WAR/Data/qid_battle.tsv"
"""
#####################merge merge the two datasets(infobox and wikidata), dropping duplicates and call them WW-data (Wiki-War Data)#################
"""
######################first step: time########################
def clean_infobox(file,file_out):
    file_out=file[file['wid']!=44131689]    
    file_out=file_out[file_out['wid']!=17677848] 
    file_out=file_out[file_out['wid']!=10343280]
    return file_out

def clean_qid(file, file_out):
    file_out=file[file['qid']!=16148654]
    file_out=file_out[file_out['qid']!=2310964] 
    file_out=file_out[file_out['qid']!=14665]
    return file_out

def add_qid(file):
    file.loc[file.wid==8401245,'qid']=4087392
    file.loc[file.wid==58664954,'qid']=60769268
    file.loc[file.wid==59361833,'qid']=60776186
   

def add_source(file_in,file_out,var,var_in,num1,num2):

    grouped_file_out=file_in[var].groupby(file_in['qid'])
    g=grouped_file_out.count()
    g=g.reset_index()
    g.rename(columns={var: 'has_country'}, inplace=True)
    file_out=pd.merge(file_in,g,on='qid',how='left')
    file_out=file_out.sort_index(by = ['qid'])
    file_out['source']=file_out['source'].fillna(-1)
    file_out.loc[(file_out.has_country!=0)&(file_out.source==-1),'source']=num1
    
#    file_out[var_in]=file_out[var_in].astype(int)
    file_out.loc[file_out.has_country==0,var]=file_out[var_in]
    file_out[var]=file_out[var].replace(0,np.nan)
    
    file_out.drop(['has_country'],axis=1, inplace=True)
    grouped_file_out=file_out[var].groupby(file_out['qid'])
    g=grouped_file_out.count()
    g=g.reset_index()
    g.rename(columns={var: 'has_country'}, inplace=True)
    file_out=pd.merge(file_out,g,on='qid',how='left')
    file_out=file_out.sort_index(by = ['qid'])
    file_out['source']=file_out['source'].fillna(-1)
    file_out.loc[(file_out.has_country!=0)&(file_out.source==-1),'source']=num2
    file_out['source']=file_out['source'].replace(-1,np.nan)

    file_out[var]=file_out[var].fillna(-1)
    file_out=file_out[file_out[var]!=-1]
    file_out.drop(['has_country',var_in],axis=1, inplace=True)
    file_out.drop_duplicates(inplace=True)
    return file_out
    
#####wikidata
qid_time_location=pd.read_csv(qid_time_location_path)#wikidata
qid_time_location.drop(['Unnamed: 0'],axis=1, inplace=True)
qid_time_location=qid_time_location[ ~ qid_time_location['qidLabel'].str.contains('Cold War')]
qid_time_location=clean_qid(qid_time_location,qid_time_location)
qid_time_location.loc[qid_time_location.qid==1151913,'end_time']=1654
qid_time_location.loc[qid_time_location.qid==40949,'end_time']=1783

grouped_qid_time_location=qid_time_location['start_time'].groupby(qid_time_location['qid'])
g=grouped_qid_time_location.min()
g=g.reset_index()# change the index to column
qid_time_location=pd.merge(qid_time_location,g,on='qid',how='left')

grouped_qid_time_location=qid_time_location['end_time'].groupby(qid_time_location['qid'])
g=grouped_qid_time_location.max()
g=g.reset_index()# change the index to column
qid_time_location=pd.merge(qid_time_location,g,on='qid',how='left')

qid_time_location.rename(columns={'start_time_y': 'start_time','end_time_y': 'end_time'}, inplace=True) 
qid_time_location.drop_duplicates(inplace=True)
wikidata_battle_qid=qid_time_location.loc[:,['qid']]
wikidata_battle_qid.drop_duplicates(inplace=True)
wikidata_battle_qid.to_csv(main_path+'tmp/wikidata_battle_qid.csv',index=False)
qid_time=qid_time_location.loc[:,['qid','qidLabel','start_time','accuracy_start','end_time','accuracy_end']]
qid_time.drop_duplicates(inplace=True)
#####
#####wiki infoboxes
info_time=pd.read_csv("D:/learning/Arash/war_participants/articles/infobox/output/wikiWar_info_time.csv")
info_time=info_time[info_time['wid']!=44131689]    
info_time=info_time[info_time['wid']!=17677848] 
info_time.loc[info_time.wid==4548076,'start_year']=-119
info_time.loc[info_time.wid==4548076,'end_year']=-119
  
info_time['accuracy_start_info']=info_time['precision']
info_time['accuracy_end_info']=info_time['precision']
info_time=info_time.loc[:,['wid','title','start_year','accuracy_start_info','end_year','accuracy_end_info']]
#match wid with qid
wid_to_qid=pd.read_csv(wid_to_qid_path,delimiter='\t')
info_time=pd.merge(info_time,wid_to_qid,on='wid',how='left')
info_time=clean_infobox(info_time,info_time)
add_qid(info_time)



info_time=info_time.loc[:,['qid','title','start_year','accuracy_start_info','end_year','accuracy_end_info']]
info_time['qid']=info_time['qid'].astype(int)
#
WWdata_time=pd.merge(info_time,qid_time,on='qid',how='outer')
WWdata_time=clean_qid(WWdata_time,WWdata_time)
  

WWdata_time=WWdata_time.fillna(0)
def fillmi(a,b):
    if a==0 and b!=0:
        return b
    else:
        return a
WWdata_time['title']=WWdata_time.apply(lambda x: fillmi(x.title,x.qidLabel), axis=1)
WWdata_time['start_year']=WWdata_time.apply(lambda x: fillmi(x.start_year,x.start_time), axis=1)
WWdata_time['accuracy_start_info']=WWdata_time.apply(lambda x: fillmi(x.accuracy_start_info,x.accuracy_start), axis=1)
WWdata_time['end_year']=WWdata_time.apply(lambda x: fillmi(x.end_year,x.end_time), axis=1)
WWdata_time['accuracy_end_info']=WWdata_time.apply(lambda x: fillmi(x.accuracy_end_info,x.accuracy_end), axis=1)


WWdata_time.loc[WWdata_time.accuracy_start=='month','start_year']=WWdata_time['start_time'] 
WWdata_time.loc[WWdata_time.accuracy_start=='month','accuracy_start_info']=WWdata_time['accuracy_start']
WWdata_time.loc[WWdata_time.accuracy_end=='month','end_year']=WWdata_time['end_time']  
WWdata_time.loc[WWdata_time.accuracy_end=='month','accuracy_end_info']=WWdata_time['accuracy_end']
WWdata_time=WWdata_time.replace(0,np.nan)
WWdata_time=WWdata_time.loc[:,['qid','title','start_year','accuracy_start_info','end_year','accuracy_end_info']]
WWdata_time.drop_duplicates(inplace=True)

WWdata_time.to_csv(main_path+'output/WWdata_time.csv',index=False)


######################################################
################second step: participant##################
#wikidata
participant=pd.read_csv(participant_path,delimiter='\t')
participant['x']=participant['qid'].str.extract(r'(\D)',expand=False)###in qid, there are some items like 'http://www.wikidata.org/entity/P1786'
participant['x']=participant['x'].fillna(0)
participant=participant[participant['x']==0]
participant.drop(['x'],axis=1, inplace=True)
participant['qid']=participant['qid'].astype(int)
wikidata_battle_qid=pd.read_csv(main_path+'tmp/wikidata_battle_qid.csv')
wd_qid_participant=pd.merge(wikidata_battle_qid,participant,on='qid',how='left' )
wd_qid_participant=wd_qid_participant.loc[:,['qid','qid_participant']]
wd_qid_participant.drop_duplicates(inplace=True)
wd_qid_participant['qid_participant']=wd_qid_participant['qid_participant'].fillna(0)
wd_qid_participant=wd_qid_participant[wd_qid_participant['qid_participant']!=0]

#infobox
war_participant_infobox=pd.read_csv(war_participant_infobox_path)
war_participant_infobox=clean_infobox(war_participant_infobox,war_participant_infobox)
war_participant_infobox=pd.merge(war_participant_infobox,wid_to_qid, on='wid',how='left')
add_qid(war_participant_infobox)

war_participant_infobox=war_participant_infobox.loc[:,['qid','title','participants','side']]

WWdata_participant=pd.merge(war_participant_infobox,wd_qid_participant,on='qid',how='outer')
WWdata_participant['qid_participant']=WWdata_participant['qid_participant'].fillna(0)
WWdata_participant['qid_participant']=WWdata_participant['qid_participant'].astype(int)
WWdata_participant['source']=np.nan

WWdata_participant=add_source(WWdata_participant,WWdata_participant,'participants','qid_participant',1,2)

WWdata_participant.to_csv(main_path+"output/WWdata_participant.csv",index=False)

##########################################################
################third step: commander#####################
#infobox
war_commander_infobox=pd.read_csv(war_commander_infobox_path)

war_commander_infobox['commander']=war_commander_infobox['commander'].fillna(0)
war_commander_infobox=war_commander_infobox[war_commander_infobox['commander']!=0]

war_commander_infobox=clean_infobox(war_commander_infobox,war_commander_infobox)
war_commander_infobox.rename(columns={'war_qid': 'qid'}, inplace=True)
add_qid(war_commander_infobox)


war_commander_infobox['source']=np.nan
grouped_file_out=war_commander_infobox['commander'].groupby(war_commander_infobox['qid'])
g=grouped_file_out.count()
g=g.reset_index()
g.rename(columns={'commander': 'has_country'}, inplace=True)
war_commander_infobox=pd.merge(war_commander_infobox,g,on='qid',how='left')
war_commander_infobox=war_commander_infobox.sort_index(by = ['qid'])
war_commander_infobox['source']=war_commander_infobox['source'].fillna(-1)
war_commander_infobox.loc[(war_commander_infobox.has_country!=0)&(war_commander_infobox.source==-1),'source']=1

war_commander_infobox.drop(['wid','has_country'],axis=1, inplace=True)

war_commander_infobox.to_csv(main_path+"output/WWdata_commander.csv",index=False)
##########################################################

################Forth step: partof########################
#wikidata
wikidata_partof=pd.read_csv("D:/learning/Arash/war_participants/material/qid_partof.csv")
wikidata_partof['war_qid']=wikidata_partof['qid'].str.extract(r'Q(\d+)',expand=False)
wikidata_partof['partof']=wikidata_partof['part_of'].str.extract(r'Q(\d+)',expand=False)
wikidata_partof=wikidata_partof.loc[:,['war_qid','part_ofLabel','partof']]
wikidata_partof.rename(columns={'war_qid': 'qid','part_ofLabel':'partof','partof':'partof_qid'}, inplace=True)
wikidata_partof['qid']=wikidata_partof['qid'].astype(int)
wikidata_battle_qid=pd.read_csv(main_path+'tmp/wikidata_battle_qid.csv')
wikidata_partof=pd.merge(wikidata_battle_qid,wikidata_partof,on='qid',how='left' )

wikidata_partof.drop_duplicates(inplace=True)
wikidata_partof['partof']=wikidata_partof['partof'].fillna(0)
wikidata_partof=wikidata_partof[wikidata_partof['partof']!=0]
wikidata_partof.drop(['partof'],axis=1, inplace=True)
wikidata_partof['partof_qid']=wikidata_partof['partof_qid'].astype(int)
#
#infobox
war_partof_infobox=pd.read_csv(war_partof_infobox_path)
war_partof_infobox=clean_infobox(war_partof_infobox,war_partof_infobox)
war_partof_infobox=pd.merge(war_partof_infobox,wid_to_qid, on='wid',how='left')
add_qid(war_partof_infobox)
war_partof_infobox.drop(['wid'],axis=1, inplace=True)




#
WWdata_partof=pd.merge(war_partof_infobox,wikidata_partof,on='qid',how='outer')
WWdata_partof['partof_qid']=WWdata_partof['partof_qid'].fillna(0)
WWdata_partof['partof_qid']=WWdata_partof['partof_qid'].astype(int)
WWdata_partof['source']=np.nan

WWdata_partof=add_source(WWdata_partof,WWdata_partof,'partof','partof_qid',1,2)
WWdata_partof=WWdata_partof.loc[:,['qid','title','partof','source']]
WWdata_partof.to_csv(main_path+"output/WWdata_partof.csv",index=False)



##########################################################


"""
############################## Graphs################################
"""
###############################average the number of wars  (start-date of the war) and plot them from 1400 onwards.#############################

WWdata_time=pd.read_csv(main_path+'output/WWdata_time.csv')
WWdata_time['start_year']=WWdata_time['start_year'].fillna(0)
WWdata_time['start_year']=[math.floor(n) for n in WWdata_time['start_year']]
WWdata_time=WWdata_time.replace(0,np.nan)




####whole world
WWdata_time_world_1400=WWdata_time[WWdata_time['start_year']>=1400]
WWdata_time_world_1400=WWdata_time_world_1400.loc[:,['qid','start_year']]

WWdata_time_world_1400['century']=WWdata_time_world_1400['start_year']/100
WWdata_time_world_1400['century']=[math.floor(n)*100 for n in WWdata_time_world_1400['century']]
WWdata_time_world_1400['diff']=WWdata_time_world_1400['start_year']-WWdata_time_world_1400['century']
WWdata_time_world_1400['quarter']=[math.floor(n/25)*25 for n in WWdata_time_world_1400['diff']]
WWdata_time_world_1400['quarter']=WWdata_time_world_1400['quarter']+WWdata_time_world_1400['century']

WWdata_time_world_1400['decade']=[math.floor(n/10)*10 for n in WWdata_time_world_1400['diff']]+WWdata_time_world_1400['century']


##across 25 years 
WWdata_time_w1400_quar=WWdata_time_world_1400['qid'].groupby(WWdata_time_world_1400['quarter'])
WWdata_time_w1400_quar=WWdata_time_w1400_quar.count()
WWdata_time_w1400_quar=WWdata_time_w1400_quar.reset_index()
WWdata_time_w1400_quar.to_csv(main_path+'output/WWdata_time_w1400_quar.csv',index=False)
## per decade
WWdata_time_w1400_dec=WWdata_time_world_1400['qid'].groupby(WWdata_time_world_1400['decade'])
WWdata_time_w1400_dec=WWdata_time_w1400_dec.count()
WWdata_time_w1400_dec=WWdata_time_w1400_dec.reset_index()
WWdata_time_w1400_dec.to_csv(main_path+'output/WWdata_time_w1400_dec.csv',index=False)

####
####Europe

WWdata_time=pd.read_csv(main_path+'output/WWdata_time.csv')
WWdata_time['start_year']=WWdata_time['start_year'].fillna(0)
WWdata_time['start_year']=[math.floor(n) for n in WWdata_time['start_year']]
WWdata_time=WWdata_time.replace(0,np.nan)

WWdata_qid_country_continent=pd.read_csv(main_path+"output/WWdata_qid_country_continent.csv")
WWdata_qid_country_continent.drop(['title'],axis=1, inplace=True)


WWdata_qid_time_place=pd.merge(WWdata_time,WWdata_qid_country_continent,on='qid',how='left')
WWdata_qid_time_place=WWdata_qid_time_place.loc[:,['qid','start_year','continent']]
eu_WWdata_qid_time_place=WWdata_qid_time_place[WWdata_qid_time_place['continent']=='Europe']
eu_WWdata_qid_time_place.drop_duplicates(inplace=True)

WWdata_time_eu_1400=eu_WWdata_qid_time_place[eu_WWdata_qid_time_place['start_year']>=1400]
WWdata_time_eu_1400=WWdata_time_eu_1400.loc[:,['qid','start_year']]

WWdata_time_eu_1400['century']=WWdata_time_eu_1400['start_year']/100
WWdata_time_eu_1400['century']=[math.floor(n)*100 for n in WWdata_time_eu_1400['century']]
WWdata_time_eu_1400['diff']=WWdata_time_eu_1400['start_year']-WWdata_time_eu_1400['century']
WWdata_time_eu_1400['quarter']=[math.floor(n/25)*25 for n in WWdata_time_eu_1400['diff']]
WWdata_time_eu_1400['quarter']=WWdata_time_eu_1400['quarter']+WWdata_time_eu_1400['century']

WWdata_time_eu_1400['decade']=[math.floor(n/10)*10 for n in WWdata_time_eu_1400['diff']]+WWdata_time_eu_1400['century']


##across 25 years 
WWdata_time_eu1400_quar=WWdata_time_eu_1400['qid'].groupby(WWdata_time_eu_1400['quarter'])
WWdata_time_eu1400_quar=WWdata_time_eu1400_quar.count()
WWdata_time_eu1400_quar=WWdata_time_eu1400_quar.reset_index()
WWdata_time_eu1400_quar.to_csv(main_path+'output/WWdata_time_eu1400_quar.csv',index=False)
## per decade
WWdata_time_eu1400_dec=WWdata_time_eu_1400['qid'].groupby(WWdata_time_eu_1400['decade'])
WWdata_time_eu1400_dec=WWdata_time_eu1400_dec.count()
WWdata_time_eu1400_dec=WWdata_time_eu1400_dec.reset_index()
WWdata_time_eu1400_dec.to_csv(main_path+'output/WWdata_time_eu1400_dec.csv',index=False)


####


########################################################################################################################################################

################################################ restrict to battles########################################
WWdata_time=pd.read_csv(main_path+'output/WWdata_time.csv')
WWdata_time['start_year']=WWdata_time['start_year'].fillna(0)
WWdata_time['start_year']=[math.floor(n) for n in WWdata_time['start_year']]
WWdata_time=WWdata_time.replace(0,np.nan)

qid_battles=pd.read_csv(qid_battle_path,delimiter='\t')

WWdata_time=pd.merge(WWdata_time,qid_battles,on='qid', how='inner')


####whole world
WWdata_time_world_1400=WWdata_time[WWdata_time['start_year']>=1400]
WWdata_time_world_1400=WWdata_time_world_1400.loc[:,['qid','start_year']]

WWdata_time_world_1400['century']=WWdata_time_world_1400['start_year']/100
WWdata_time_world_1400['century']=[math.floor(n)*100 for n in WWdata_time_world_1400['century']]
WWdata_time_world_1400['diff']=WWdata_time_world_1400['start_year']-WWdata_time_world_1400['century']
WWdata_time_world_1400['quarter']=[math.floor(n/25)*25 for n in WWdata_time_world_1400['diff']]
WWdata_time_world_1400['quarter']=WWdata_time_world_1400['quarter']+WWdata_time_world_1400['century']

WWdata_time_world_1400['decade']=[math.floor(n/10)*10 for n in WWdata_time_world_1400['diff']]+WWdata_time_world_1400['century']


##across 25 years 
WWdata_time_w1400_quar=WWdata_time_world_1400['qid'].groupby(WWdata_time_world_1400['quarter'])
WWdata_time_w1400_quar=WWdata_time_w1400_quar.count()
WWdata_time_w1400_quar=WWdata_time_w1400_quar.reset_index()
WWdata_time_w1400_quar.to_csv(main_path+'output/WWdata_time_battles_w1400_quar.csv',index=False)
## per decade
WWdata_time_w1400_dec=WWdata_time_world_1400['qid'].groupby(WWdata_time_world_1400['decade'])
WWdata_time_w1400_dec=WWdata_time_w1400_dec.count()
WWdata_time_w1400_dec=WWdata_time_w1400_dec.reset_index()
WWdata_time_w1400_dec.to_csv(main_path+'output/WWdata_time_battles_w1400_dec.csv',index=False)

####
####Europe

WWdata_time=pd.read_csv(main_path+'output/WWdata_time.csv')
WWdata_time['start_year']=WWdata_time['start_year'].fillna(0)
WWdata_time['start_year']=[math.floor(n) for n in WWdata_time['start_year']]
WWdata_time=WWdata_time.replace(0,np.nan)

qid_battles=pd.read_csv(qid_battle_path,delimiter='\t')

WWdata_time=pd.merge(WWdata_time,qid_battles,on='qid', how='inner')

WWdata_qid_country_continent=pd.read_csv(main_path+"output/WWdata_qid_country_continent.csv")
WWdata_qid_country_continent.drop(['title'],axis=1, inplace=True)


WWdata_qid_time_place=pd.merge(WWdata_time,WWdata_qid_country_continent,on='qid',how='left')
WWdata_qid_time_place=WWdata_qid_time_place.loc[:,['qid','start_year','continent']]
eu_WWdata_qid_time_place=WWdata_qid_time_place[WWdata_qid_time_place['continent']=='Europe']
eu_WWdata_qid_time_place.drop_duplicates(inplace=True)

WWdata_time_eu_1400=eu_WWdata_qid_time_place[eu_WWdata_qid_time_place['start_year']>=1400]
WWdata_time_eu_1400=WWdata_time_eu_1400.loc[:,['qid','start_year']]

WWdata_time_eu_1400['century']=WWdata_time_eu_1400['start_year']/100
WWdata_time_eu_1400['century']=[math.floor(n)*100 for n in WWdata_time_eu_1400['century']]
WWdata_time_eu_1400['diff']=WWdata_time_eu_1400['start_year']-WWdata_time_eu_1400['century']
WWdata_time_eu_1400['quarter']=[math.floor(n/25)*25 for n in WWdata_time_eu_1400['diff']]
WWdata_time_eu_1400['quarter']=WWdata_time_eu_1400['quarter']+WWdata_time_eu_1400['century']

WWdata_time_eu_1400['decade']=[math.floor(n/10)*10 for n in WWdata_time_eu_1400['diff']]+WWdata_time_eu_1400['century']


##across 25 years 
WWdata_time_eu1400_quar=WWdata_time_eu_1400['qid'].groupby(WWdata_time_eu_1400['quarter'])
WWdata_time_eu1400_quar=WWdata_time_eu1400_quar.count()
WWdata_time_eu1400_quar=WWdata_time_eu1400_quar.reset_index()
WWdata_time_eu1400_quar.to_csv(main_path+'output/WWdata_time_battles_eu1400_quar.csv',index=False)
## per decade
WWdata_time_eu1400_dec=WWdata_time_eu_1400['qid'].groupby(WWdata_time_eu_1400['decade'])
WWdata_time_eu1400_dec=WWdata_time_eu1400_dec.count()
WWdata_time_eu1400_dec=WWdata_time_eu1400_dec.reset_index()
WWdata_time_eu1400_dec.to_csv(main_path+'output/WWdata_time_battles_eu1400_dec.csv',index=False)

############################################################################################################
WWdata_time_w1400_quar=pd.read_csv(main_path+'output/WWdata_time_w1400_quar.csv')
WWdata_time_w1400_dec=pd.read_csv(main_path+'output/WWdata_time_w1400_dec.csv')
WWdata_time_eu1400_quar=pd.read_csv(main_path+'output/WWdata_time_eu1400_quar.csv')
WWdata_time_eu1400_dec=pd.read_csv(main_path+'output/WWdata_time_eu1400_dec.csv')

WWdata_time_battles_w1400_quar=pd.read_csv(main_path+'output/WWdata_time_battles_w1400_quar.csv')
WWdata_time_battles_w1400_dec=pd.read_csv(main_path+'output/WWdata_time_battles_w1400_dec.csv')
WWdata_time_battles_eu1400_quar=pd.read_csv(main_path+'output/WWdata_time_battles_eu1400_quar.csv')
WWdata_time_battles_eu1400_dec=pd.read_csv(main_path+'output/WWdata_time_battles_eu1400_dec.csv')

WWdata_time_w1400_quar.rename(columns={'qid': 'all_conflicts'}, inplace=True)
WWdata_time_w1400_dec.rename(columns={'qid': 'all_conflicts'}, inplace=True)
WWdata_time_eu1400_quar.rename(columns={'qid': 'all_conflicts'}, inplace=True)
WWdata_time_eu1400_dec.rename(columns={'qid': 'all_conflicts'}, inplace=True)

WWdata_time_battles_w1400_quar.rename(columns={'qid': 'only_battles'}, inplace=True)
WWdata_time_battles_w1400_dec.rename(columns={'qid': 'only_battles'}, inplace=True)
WWdata_time_battles_eu1400_quar.rename(columns={'qid': 'only_battles'}, inplace=True)
WWdata_time_battles_eu1400_dec.rename(columns={'qid': 'only_battles'}, inplace=True)

WWdata_time_w1400_quar=pd.merge(WWdata_time_w1400_quar,WWdata_time_battles_w1400_quar, on='quarter', how='outer')
WWdata_time_w1400_dec=pd.merge(WWdata_time_w1400_dec,WWdata_time_battles_w1400_dec, on='decade', how='outer')
WWdata_time_eu1400_quar=pd.merge(WWdata_time_eu1400_quar,WWdata_time_battles_eu1400_quar, on='quarter', how='outer')
WWdata_time_eu1400_dec=pd.merge(WWdata_time_eu1400_dec,WWdata_time_battles_eu1400_dec, on='decade', how='outer')

WWdata_time_w1400_quar.to_csv(main_path+'output/WWdata_time_w1400_quar.csv', index=False)
WWdata_time_w1400_dec.to_csv(main_path+'output/WWdata_time_w1400_dec.csv', index=False)
WWdata_time_eu1400_quar.to_csv(main_path+'output/WWdata_time_eu1400_quar.csv', index=False)
WWdata_time_eu1400_dec.to_csv(main_path+'output/WWdata_time_eu1400_dec.csv', index=False)