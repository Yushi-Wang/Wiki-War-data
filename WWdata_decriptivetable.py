# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 14:20:00 2019
This file is for getting the discriptivetable of WW data
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
from os import mkdir
main_path='D:/learning/Arash/war_participants/Wiki-War Data/'


try:
    mkdir(main_path)
except FileExistsError:
    pass
now = datetime.datetime.now()
time=str(now)
time=time[0:10]
missingtable_path=main_path+'tmp/WWdata_descriptive_table'+time+'.xls'

pariticipants_path=main_path+'output/WWdata_participant.csv'
commanders_path=main_path+'output/WWdata_commander.csv'
time_path=main_path+'output/WWdata_time.csv'
location_path=main_path+'output/WWdata_qid_country_continent.csv'
partof_path=main_path+'output/WWdata_partof.csv'
wid_to_qid_path='D:/learning/Arash/war_participants/articles/infobox/input/wid_to_qid.tsv'

def add_qid(file):
    file.loc[file.wid==8401245,'qid']=4087392
    file.loc[file.wid==58664954,'qid']=60769268
    file.loc[file.wid==59361833,'qid']=60776186



writer=pd.read_csv('D:/learning/Arash/war_participants/articles/infobox/input/infobox_new.csv') 
writer=writer[writer['wid']!=288520]
writer=writer[writer['wid']!=160665]
writer=writer[writer['wid']!=160664]
writer=writer[writer['wid']!=560948]
writer=writer[writer['wid']!=2150520]
writer=writer[writer['wid']!=16315254]
writer=writer[writer['wid']!=207630]
writer=writer[writer['wid']!=205658]
writer=writer[writer['wid']!=338949]
writer=writer[writer['wid']!=896446]
writer=writer[writer['wid']!=44131689]
writer=writer[writer['wid']!=17677848]
writer=writer[writer['wid']!=10343280]

writer=writer.loc[:,['wid']]
wid_to_qid=pd.read_csv(wid_to_qid_path,delimiter='\t')
writer=pd.merge(writer,wid_to_qid,on='wid',how='left')
add_qid(writer)

writer=writer.loc[:,['qid']]
wikidata_battle_qid=pd.read_csv(main_path+'tmp/wikidata_battle_qid.csv')
WWdata_qid=pd.merge(wikidata_battle_qid,writer,on='qid',how='outer')
WWdata_qid.to_csv(main_path+'tmp/WWdata_qid.csv', index=False)

####number of battles
all_num_ob=len(np.unique(WWdata_qid['qid']))
#### participants
WWdata_participant=pd.read_csv(pariticipants_path) 
WWdata_participant['participants']=WWdata_participant['participants'].fillna(0)
WWdata_participant=WWdata_participant[WWdata_participant['participants']!=0]
num_participant=len(np.unique(WWdata_participant['qid']))
percent_participant=num_participant/all_num_ob
percent_participant=format(percent_participant, '.2%')

####commanders
WWdata_commander=pd.read_csv(commanders_path) 
WWdata_commander['commander']=WWdata_commander['commander'].fillna(0)
WWdata_com=WWdata_commander[WWdata_commander['commander']!=0]
num_com=len(np.unique(WWdata_com['qid']))
percent_com=num_com/all_num_ob
percent_com=format(percent_com, '.2%')
####percent of start time information
WWdata_time=pd.read_csv(time_path) 
start_t_g=WWdata_time['start_year'].groupby(WWdata_time['qid'])
s_t_g=start_t_g.count()
num_start_t=s_t_g[s_t_g.isin([0])]
num_start_t=len(num_start_t)
percent_start_time=1-num_start_t/all_num_ob
percent_start_time=format(percent_start_time, '.2%')

####percent of end time information
end_t_g=WWdata_time['end_year'].groupby(WWdata_time['qid'])
e_t_g=end_t_g.count()
num_end_t=e_t_g[e_t_g.isin([0])]
num_end_t=len(num_end_t)
percent_end_time=1-num_end_t/all_num_ob
percent_end_time=format(percent_end_time, '.2%')

####location
#country
WWdata_location=pd.read_csv(location_path)
WWdata_location=pd.merge(WWdata_qid,WWdata_location,on='qid',how='left')
WWdata_location['country']=WWdata_location['country'].fillna(0)
WWdata_location=WWdata_location[WWdata_location['country']!=0]
num_country=len(np.unique(WWdata_location['qid']))
percent_country=num_country/all_num_ob
percent_country=format(percent_country, '.2%')

#continent
WWdata_location=pd.read_csv(location_path)
WWdata_location=pd.merge(WWdata_qid,WWdata_location,on='qid',how='left')
WWdata_location['continent']=WWdata_location['continent'].fillna(0)
WWdata_location=WWdata_location[WWdata_location['continent']!=0]
num_continent=len(np.unique(WWdata_location['qid']))
percent_continent=num_continent/all_num_ob
percent_continent=format(percent_continent, '.2%')

#### partof
WWdata_partof=pd.read_csv(partof_path)

WWdata_partof['partof']=WWdata_partof['partof'].fillna(0)
WWdata_partof=WWdata_partof[WWdata_partof['partof']!=0]
num_partof=len(np.unique(WWdata_partof['qid']))
percent_partof=num_partof/all_num_ob
percent_partof=format(percent_partof, '.2%')

data={'Wiki-War_data':[percent_start_time,percent_end_time,percent_participant,percent_com,percent_country,percent_continent,percent_partof,all_num_ob]}
misssing_table=pd.DataFrame(data,index=['Start_time','End_time','Participant','Commander','involved country','involved continent','Partof','Number_of_battles'])
misssing_table.to_excel(missingtable_path)
