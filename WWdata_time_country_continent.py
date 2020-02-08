# -*- coding: utf-8 -*-
"""
Created on Sat Oct 12 19:51:36 2019
This file is for matching battles with countries and continents. I reviewed the tmp_WARS.do and used the method that Fabian used to
 do this and only did some small changes. First, I kept those items that have a qidLabel with a format “Q1234”. Second, I used the 
 country_ag in qid_time_location.csv as an additional source of the country information and use “7” in “country_source” to indicate this source.

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
import math
from os import mkdir
main_path='D:/learning/Arash/war_participants/Wiki-War Data/'
loadin_path = "D:/learning/Arash/war_participants/Fabian_06292019_WAR/"
qidAltSubject_en_path=loadin_path+"Data/A1_00_qidAltSubject_en.dta"
qidSubject_path=loadin_path+"Data/A1_00_qidSubject.dta"
qid_capital_country_path=loadin_path+"Data/qid_capital.tsv"
country_map_in_path=loadin_path+"Data/A1_05_03_mapCountry.dta"
#wid_to_qid_path=loadin_path+"Data/wid_to_qid.tsv"
wid_to_qid_path='D:/learning/Arash/war_participants/articles/infobox/input/wid_to_qid.tsv'
qid_battle_path=loadin_path+"Data/qid_battle.tsv"
qid_time_location_path=loadin_path+"Data/qid_time_location.csv"
info_time_path="D:/learning/Arash/war_participants/articles/infobox/output/wikiWar_info_time.csv"
participant_path=loadin_path+"Data/participant.tsv"
#war_commander_infobox_path=loadin_path+"Data/war_commander_infobox.csv"
war_commander_infobox_path="D:/learning/Arash/war_participants/articles/infobox/output/wikiWAR_commander_infobox.csv"
#war_participant_infobox_path=loadin_path+"Data/war_participant_infobox.csv"
war_participant_infobox_path="D:/learning/Arash/war_participants/articles/infobox/output/wikiWAR_participant_infobox.csv"
mapContinent_path=loadin_path+"Data/A1_05_03_mapContinent.dta"
######Time from wikidata
qid_time_location=pd.read_csv(qid_time_location_path)


###### restrict sample to battles
######merge n:1 qid using tmp/qid_battle.dta, keep(3) nogen
qid_time_location.drop(['country_ag', 'location_ag', 'continent_ag','Unnamed: 0'],axis=1, inplace=True)
qid_time_location=qid_time_location[ ~ qid_time_location['qidLabel'].str.contains('Cold War')]

def month(a,b):
    if b=="month":
        return math.floor(a)
    else:
        return a
    
qid_time_location['start_time']=qid_time_location.apply(lambda x: month(x.start_time,x.accuracy_start), axis=1)    
qid_time_location['end_time']=qid_time_location.apply(lambda x: month(x.end_time,x.accuracy_end), axis=1)    


qid_time_location.loc[qid_time_location.accuracy_start=='month','accuracy_start']='year'
qid_time_location.loc[qid_time_location.accuracy_end=='month','accuracy_end']='year'

qid_time_location=qid_time_location[qid_time_location['qid']!=2310964]    
qid_time_location.loc[qid_time_location.qid==1151913,'end_time']=1654
qid_time_location.loc[qid_time_location.qid==40949,'end_time']=1783

qid_time_location.drop(['start_time_origin', 'end_time_origin'],axis=1, inplace=True)
"""
########### keep the qids with a qidLabel with a format "Q1234"
"""
####qid_time_location['comb1_2']=qid_time_location['qidLabel'].str.extract(r'(Q\d\d)',expand=False)
####qid_time_location['comb1_2']=qid_time_location['comb1_2'].fillna(0)
####qid_time_location=qid_time_location[qid_time_location['comb1_2']==0]
####qid_time_location.drop(['comb1_2'],axis=1, inplace=True)
"""
###########
"""
grouped_qid_time_location=qid_time_location['start_time'].groupby(qid_time_location['qid'])
g=grouped_qid_time_location.min()
g=g.reset_index()# change the index to column
qid_time_location=pd.merge(qid_time_location,g,on='qid',how='left')

grouped_qid_time_location=qid_time_location['end_time'].groupby(qid_time_location['qid'])
g=grouped_qid_time_location.max()
g=g.reset_index()# change the index to column
qid_time_location=pd.merge(qid_time_location,g,on='qid',how='left')

qid_time_location.rename(columns={'start_time_y': 'start_time','end_time_y': 'end_time'}, inplace=True) 
qid_time_location=qid_time_location.loc[:,['qid','qidLabel','start_time','accuracy_start','end_time','accuracy_end']]
qid_time_location.drop_duplicates(inplace=True)
qid_time_location.to_csv(main_path+"tmp/WAR_wd.csv",index=False)


######Time from infoboxes + wd
info_time=pd.read_csv(info_time_path)
info_time=info_time[info_time['wid']!=44131689]    
info_time=info_time[info_time['wid']!=17677848]

info_time=info_time[info_time['wid']!=10343280]    
info_time.loc[info_time.wid==4548076,'start_year']=-119
info_time.loc[info_time.wid==4548076,'end_year']=-119

wid_to_qid=pd.read_csv(wid_to_qid_path,delimiter='\t')
info_time=pd.merge(info_time,wid_to_qid,on='wid',how='left')
info_time.loc[info_time.wid==8401245,'qid']=4087392
info_time.loc[info_time.wid==58664954,'qid']=60769268
info_time.loc[info_time.wid==59361833,'qid']=60776186
info_time.drop(['wid'],axis=1, inplace=True)

######restrict sample to battles
qid_battle=pd.read_csv(qid_battle_path,delimiter='\t')
qid_battle['battle']=1
info_time=pd.merge(info_time,qid_battle,on='qid',how='left')
info_time=info_time[info_time['battle']==1]
info_time=pd.merge(info_time,qid_time_location,on='qid',how='outer')
info_time.drop(['battle'],axis=1, inplace=True)

info_time=info_time[info_time['qid']!=859401]    
info_time=info_time[info_time['qid']!=899191]    
info_time=info_time[info_time['qid']!=524272]    
info_time=info_time[info_time['qid']!=16148654]  
info_time=info_time[info_time['qid']!=31406]  

info_time['title']=info_time['title'].fillna(0)
info_time['qidLabel']=info_time['qidLabel'].fillna(0)
def fillmi(a,b):
    if a==0 and b!=0:
        return b
    else:
        return a
info_time['title']=info_time.apply(lambda x: fillmi(x.title,x.qidLabel), axis=1)
info_time.drop(['qidLabel'],axis=1, inplace=True)
info_time['title']=info_time['title'].replace(0,np.nan)

def function1(a,b,c):
    if a=="year" and b!="year":
        return np.nan
    else:
        return c
info_time['start_time']=info_time.apply(lambda x: function1(x.precision,x.accuracy_start,x.start_time), axis=1)
info_time['end_time']=info_time.apply(lambda x: function1(x.precision,x.accuracy_end,x.end_time), axis=1)

info_time['end_time']=info_time['end_time'].fillna(0)
info_time['start_time']=info_time['start_time'].fillna(0)
info_time.loc[info_time.end_time!=0,'precision']=info_time['accuracy_end']
info_time.loc[info_time.start_time!=0,'precision']=info_time['accuracy_start']
info_time.loc[info_time.start_time!=0,'start_year']=info_time['start_time']
info_time.loc[info_time.end_time!=0,'end_year']=info_time['end_time']

info_time.drop(['accuracy_end', 'accuracy_start', 'start_time', 'end_time'],axis=1, inplace=True)
      
info_time.loc[info_time.qid==1940446,'start_year']=1685
info_time.loc[info_time.qid==805050,'start_year']=581
info_time.loc[info_time.qid==805050,'end_year']=602

info_time['dif']=info_time['end_year']-info_time['start_year']
info_time['dif']=info_time['dif'].fillna(0)
info_time=info_time[info_time['dif']>=0]    
info_time.drop(['dif'],axis=1, inplace=True)

info_time=info_time.sort_index(by = ['qid'])
"""
####info_time.to_csv(main_path+"tmp/WAR_info_time_place.csv",index=False)
"""
#######For commanders
info_time=info_time.loc[:,['title','qid']]
info_time.rename(columns={'title': 'partof'}, inplace=True) 
info_time['partof']=info_time['partof'].str.lower()
info_time['partof']=info_time['partof'].str.strip()

info_time['_n']=info_time['qid'].groupby(info_time['partof']).rank(ascending=1,method='first')
info_time=info_time[info_time['_n']==1]    
info_time.drop(['_n'],axis=1, inplace=True)

info_time.to_csv(main_path+"tmp/WAR_qid_partofonly.csv",index=False)

######location from wikidata
qid_battle_locInfo=pd.read_csv(loadin_path+"Data/qid_battle_locInfo.tsv",delimiter='\t')
qid_battle_locInfo=qid_battle_locInfo.fillna(-1)

######country from country (wikidata)
qid_battle_locInfo.rename(columns={'qid': 'qid_battle','qid_country': 'qid'}, inplace=True)
qidSubject=pd.read_stata(qidSubject_path)
qidSubject['subject']=qidSubject['subject'].str.encode('latin-1').str.decode('utf-8')
qid_battle_locInfo=pd.merge(qid_battle_locInfo,qidSubject,on='qid',how='left')
qid_battle_locInfo.rename(columns={'subject': 'location'}, inplace=True)

country_map_in=pd.read_stata(country_map_in_path)
country_map_in['location']=country_map_in['location'].str.encode('latin-1').str.decode('utf-8')
country_map_in['country']=country_map_in['country'].str.encode('latin-1').str.decode('utf-8')
qid_battle_locInfo=pd.merge(qid_battle_locInfo,country_map_in,on='location',how='left')
qid_battle_locInfo.rename(columns={'location': 'locwd_country','country': 'countrywd_country'}, inplace=True)
qid_battle_locInfo.drop(['qid'],axis=1, inplace=True)

######country from location (wikidata)
qid_battle_locInfo.rename(columns={'qid_location': 'qid'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,qidSubject,on='qid',how='left')
qid_battle_locInfo.rename(columns={'subject': 'location'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,country_map_in,on='location',how='left')
qid_battle_locInfo.rename(columns={'location': 'locwd_location','country': 'countrywd_location'}, inplace=True)
qid_battle_locInfo.drop(['qid'],axis=1, inplace=True)

######country from location -> country (wikidata)
qid_battle_locInfo.rename(columns={'qid_location_country': 'qid'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,qidSubject,on='qid',how='left')
qid_battle_locInfo.rename(columns={'subject': 'location'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,country_map_in,on='location',how='left')
qid_battle_locInfo.rename(columns={'location': 'locwd_locationcountry','country': 'countrywd_locationcountry'}, inplace=True)
qid_battle_locInfo.drop(['qid'],axis=1, inplace=True)

######country from administrative region
qid_battle_locInfo.rename(columns={'qid_admEnt': 'qid'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,qidSubject,on='qid',how='left')
qid_battle_locInfo.rename(columns={'subject': 'location'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,country_map_in,on='location',how='left')
qid_battle_locInfo.rename(columns={'location': 'locwd_adment','country': 'countrywd_adment'}, inplace=True)
qid_battle_locInfo.drop(['qid'],axis=1, inplace=True)

######country from administrative region -> country
qid_battle_locInfo.rename(columns={'qid_admEnt_country': 'qid'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,qidSubject,on='qid',how='left')
qid_battle_locInfo.rename(columns={'subject': 'location'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,country_map_in,on='location',how='left')
qid_battle_locInfo.rename(columns={'location': 'locwd_admentc','country': 'countrywd_admentc'}, inplace=True)
qid_battle_locInfo.drop(['qid'],axis=1, inplace=True)

######country from capital 
qid_battle_locInfo.rename(columns={'qid_capital': 'qid'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,qidSubject,on='qid',how='left')
qid_battle_locInfo.rename(columns={'subject': 'location'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,country_map_in,on='location',how='left')
qid_battle_locInfo.rename(columns={'location': 'locwd_capital','country': 'countrywd_capital'}, inplace=True)
qid_battle_locInfo.drop(['qid'],axis=1, inplace=True)

######country from capital -> country
qid_battle_locInfo.rename(columns={'qid_capital_country': 'qid'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,qidSubject,on='qid',how='left')
qid_battle_locInfo.rename(columns={'subject': 'location'}, inplace=True)
qid_battle_locInfo=pd.merge(qid_battle_locInfo,country_map_in,on='location',how='left')
qid_battle_locInfo.rename(columns={'location': 'locwd_capital_country','country': 'countrywd_capital_country'}, inplace=True)
qid_battle_locInfo.drop(['qid'],axis=1, inplace=True)

qid_battle_locInfo=qid_battle_locInfo.sort_index(by = ['qid_battle'])
qid_battle_locInfo.to_csv(main_path+"tmp/WAR_wd_country_diagnostic.csv",index=False)

qid_battle_locInfo.drop(['locwd_country', 'locwd_location','locwd_locationcountry','locwd_adment','locwd_admentc','locwd_capital','locwd_capital_country'],axis=1, inplace=True)

#foreach var of varlist countrywd_* {
#rename `var' country
#simplify_country
#aggregate_countries
#rename country `var'
#drop fine_country _country_old
#}
qid_battle_locInfo=qid_battle_locInfo.reset_index()
qid_battle_locInfo.drop(['index'],axis=1, inplace=True)
qid_battle_locInfo=qid_battle_locInfo.reset_index()
qid_battle_locInfo.rename(columns={'index': 'id'}, inplace=True)
qid_battle_locInfo_long=pd.melt(qid_battle_locInfo,id_vars=['id','qid_battle'])
qid_battle_locInfo_long.drop(['id'],axis=1, inplace=True)
qid_battle_locInfo_long=qid_battle_locInfo_long.fillna(0)
qid_battle_locInfo_long=qid_battle_locInfo_long[qid_battle_locInfo_long['value']!=0]
qid_battle_locInfo_long.drop_duplicates(inplace=True)
qid_battle_locInfo_long=qid_battle_locInfo_long.sort_index(by = ['qid_battle'])
qid_battle_locInfo_long.rename(columns={'value': 'country','variable':'source'}, inplace=True)
qid_battle_locInfo_long['source']=qid_battle_locInfo_long['source'].str.replace(r'countrywd_','')
qid_battle_locInfo_long.to_csv(main_path+"tmp/WAR_wd_country_diagnostic2.csv",index=False)


######for now keep all
qid_battle_locInfo_long.drop(['source'],axis=1, inplace=True)
qid_battle_locInfo_long.drop_duplicates(inplace=True)
qid_battle_locInfo_long.rename(columns={'qid_battle': 'qid'}, inplace=True)
qid_battle_locInfo_long.to_csv(main_path+"tmp/WAR_country_wdloc.csv",index=False)


######Participants from wikidata
participant=pd.read_csv(participant_path,delimiter='\t')
participant['x']=participant['qid'].str.extract(r'(\D)',expand=False)###in qid, there are some items like 'http://www.wikidata.org/entity/P1786'
participant['x']=participant['x'].fillna(0)
participant=participant[participant['x']==0]
participant.drop(['x'],axis=1, inplace=True)
participant['qid']=participant['qid'].astype(int)
qid_battle=pd.read_csv(qid_battle_path,delimiter='\t')
participant=pd.merge(participant,qid_battle,on='qid',how='inner')

WAR_qid_partofonly=pd.read_csv(main_path+'tmp/WAR_qid_partofonly.csv')
participant=pd.merge(participant,WAR_qid_partofonly,on='qid',how='inner')
participant.drop(['partof'],axis=1, inplace=True)
participant=participant.sort_index(by = ['qid'])
participant.to_csv(main_path+"tmp/WAR_wd_participant.csv",index=False)


######Participants from infoboxes
qidAltSubject_en=pd.read_stata(qidAltSubject_en_path)
qidAltSubject_en['altSubject']=qidAltSubject_en['altSubject'].str.encode('latin-1').str.decode('utf-8')####Atention: This step is needed or there will be a lot of items unmatched
qidAltSubject_en.rename(columns={'altSubject': 'subject'}, inplace=True)
qidAltSubject_en_app=qidAltSubject_en.append(qidSubject,ignore_index=True)

qidAltSubject_en_app['x']=qidAltSubject_en_app['subject'].str.extract(r'(/)',expand=False)
qidAltSubject_en_app['x']=qidAltSubject_en_app['x'].fillna(0)
qidAltSubject_en_app=qidAltSubject_en_app[qidAltSubject_en_app['x']==0]
qidAltSubject_en_app.drop(['x'],axis=1, inplace=True)

qidAltSubject_en_app['participants']=qidAltSubject_en_app['subject'].str.lower()
qidAltSubject_en_app['participants']=qidAltSubject_en_app['participants'].str.replace(r'_',' ')
qidAltSubject_en_app.drop(['subject'],axis=1, inplace=True)
qidAltSubject_en_app=qidAltSubject_en_app.sort_index(by = ['participants'])
qidAltSubject_en_app.rename(columns={'qid': 'qid_participants'}, inplace=True)
qidAltSubject_en_app.to_csv(main_path+"tmp/qid_participants_subject.csv",index=False)

war_commander_infobox=pd.read_csv(war_commander_infobox_path)
war_commander_infobox['commander']=war_commander_infobox['commander'].str.strip()
war_commander_infobox['commander']=war_commander_infobox['commander'].str.lower()
war_commander_infobox['commander']=war_commander_infobox['commander'].str.replace(r' ','_')
war_commander_infobox.drop(['commander_of_1','commander_of_2','commander_of_3','commander_of_4','side'],axis=1, inplace=True)
war_commander_infobox['commander']=war_commander_infobox['commander'].fillna(0)
war_commander_infobox=war_commander_infobox[war_commander_infobox['commander']!=0]
war_commander_infobox=pd.merge(war_commander_infobox,wid_to_qid,on='wid',how='left')
war_commander_infobox.loc[war_commander_infobox.wid==8401245,'qid']=4087392
war_commander_infobox.loc[war_commander_infobox.wid==58664954,'qid']=60769268
war_commander_infobox.loc[war_commander_infobox.wid==59361833,'qid']=60776186
war_commander_infobox['qid']=war_commander_infobox['qid'].fillna(0)
war_commander_infobox=war_commander_infobox[war_commander_infobox['qid']!=0]
war_commander_infobox.drop(['wid'],axis=1, inplace=True)
war_commander_infobox.to_csv(main_path+"tmp/WAR_commander.csv",index=False)


war_participant_infobox=pd.read_csv(war_participant_infobox_path)
war_participant_infobox=pd.merge(war_participant_infobox,wid_to_qid,on='wid',how='left')
war_participant_infobox.loc[war_participant_infobox.wid==8401245,'qid']=4087392
war_participant_infobox.loc[war_participant_infobox.wid==58664954,'qid']=60769268
war_participant_infobox.loc[war_participant_infobox.wid==59361833,'qid']=60776186
war_participant_infobox['qid']=war_participant_infobox['qid'].fillna(0)
war_participant_infobox=war_participant_infobox[war_participant_infobox['qid']!=0]
war_participant_infobox.drop(['wid'],axis=1, inplace=True)
war_participant_infobox['participants']=war_participant_infobox['participants'].str.replace(r'#.*','')
war_participant_infobox=pd.merge(war_participant_infobox,qidAltSubject_en_app,on='participants',how='inner')
war_participant_infobox.drop(['side', 'participants'],axis=1, inplace=True)
war_participant_infobox.drop_duplicates(inplace=True)
war_participant_infobox=war_participant_infobox.sort_index(by = ['qid','qid_participants']) 
war_participant_infobox.rename(columns={'qid': 'qid_war','qid_participants':'qid'}, inplace=True)
war_participant_infobox.to_csv(main_path+"tmp/tmp_before_merge.csv",index=False)


qid_capital=pd.read_csv(qid_capital_country_path,delimiter='\t')
tmp_before_merge=pd.read_csv(main_path+'tmp/tmp_before_merge.csv')
tmp_before_merge=pd.merge(tmp_before_merge,qid_capital,on='qid',how='left')#change 'inner' to 'left'
tmp_before_merge.rename(columns={'qid': 'qid_participant','qid_war':'qid'}, inplace=True)
tmp_before_merge.drop_duplicates(inplace=True)
tmp_before_merge.to_csv(main_path+"tmp/WAR_info_participant.csv",index=False)

#combine wd participants and infobox participants
WAR_wd_participant=pd.read_csv(main_path+"tmp/WAR_wd_participant.csv")
WAR_diagnosis_participant=WAR_wd_participant.append(tmp_before_merge,ignore_index=True)

#cleanup:
WAR_diagnosis_participant=WAR_diagnosis_participant[WAR_diagnosis_participant['qid_participant']!=7325]
WAR_diagnosis_participant=WAR_diagnosis_participant[WAR_diagnosis_participant['qid_participant']!=56276799]

#not used
WAR_diagnosis_participant.drop(['qid_has_part'],axis=1, inplace=True)
WAR_diagnosis_participant.drop_duplicates(inplace=True)


#bring in country by capitals.
WAR_diagnosis_participant.rename(columns={'qid_capital': 'qid','qid':'qid_war'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,qidSubject,on='qid',how='left')
WAR_diagnosis_participant.rename(columns={'subject': 'location'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,country_map_in,on='location',how='left')
WAR_diagnosis_participant.rename(columns={'location': 'location_capital'}, inplace=True)
WAR_diagnosis_participant.drop(['location_capital','qid'],axis=1, inplace=True) 


#bring in country by location
WAR_diagnosis_participant.rename(columns={'qid_location': 'qid','country':'country_capital'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,qidSubject,on='qid',how='left')
WAR_diagnosis_participant.rename(columns={'subject': 'location'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,country_map_in,on='location',how='left')
WAR_diagnosis_participant.rename(columns={'location': 'location_location'}, inplace=True)
WAR_diagnosis_participant.drop(['qid'],axis=1, inplace=True) 

#bring in country by country (on wikidata page)
WAR_diagnosis_participant.rename(columns={'qid_country': 'qid','country':'country_location'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,qidSubject,on='qid',how='left')
WAR_diagnosis_participant['qid']=WAR_diagnosis_participant['qid'].fillna(0)#*
WAR_diagnosis_participant['subject']=WAR_diagnosis_participant['subject'].fillna(0)
WAR_diagnosis_participant=WAR_diagnosis_participant[(WAR_diagnosis_participant['qid']==0) | (WAR_diagnosis_participant['subject']!=0)]
WAR_diagnosis_participant['qid']=WAR_diagnosis_participant['qid'].replace(0,np.nan)
WAR_diagnosis_participant['subject']=WAR_diagnosis_participant['subject'].replace(0,np.nan)#* these steps may cause a lose of several wars
WAR_diagnosis_participant.rename(columns={'subject': 'location'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,country_map_in,on='location',how='left')
WAR_diagnosis_participant.rename(columns={'location':'location_countrywd'}, inplace=True)
WAR_diagnosis_participant.drop(['qid'],axis=1, inplace=True)

#bring in country by participant (participant is an exact country)
WAR_diagnosis_participant.rename(columns={'qid_participant': 'qid','country':'country_country'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,qidSubject,on='qid',how='left')
WAR_diagnosis_participant.rename(columns={'subject': 'location'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,country_map_in,on='location',how='left')
WAR_diagnosis_participant.rename(columns={'location': 'location_participant','qid':'qid_participant'}, inplace=True)

WAR_diagnosis_participant.drop_duplicates(inplace=True)
WAR_diagnosis_participant.to_csv(main_path+"tmp/WAR_diagnosis_participant.csv",index=False)

WAR_diagnosis_participant.drop(['location_location','location_participant','location_countrywd'],axis=1, inplace=True)
grouped_WAR_diagnosis_participant=WAR_diagnosis_participant['country'].groupby(WAR_diagnosis_participant['qid_participant'])
WAR_diagnosis_participant['country_source']=np.nan
g=grouped_WAR_diagnosis_participant.count()
g=g.reset_index()# change the index to column
g.rename(columns={'country': 'has_country'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,g,on='qid_participant',how='left')
WAR_diagnosis_participant=WAR_diagnosis_participant.sort_index(by = ['qid_participant'])
WAR_diagnosis_participant['country_source']=WAR_diagnosis_participant['country_source'].fillna(0)

def function2(a,b):
    if b!=0:
        return 1
    else:
        return a
WAR_diagnosis_participant['country_source']=WAR_diagnosis_participant.apply(lambda x: function2(x.country_source,x.has_country), axis=1)

def function3(a,b,c):
    if b==0:
        return a
    else:
        return c
WAR_diagnosis_participant['country']=WAR_diagnosis_participant.apply(lambda x: function3(x.country_country,x.has_country,x.country), axis=1)
WAR_diagnosis_participant.drop(['has_country', 'country_country'],axis=1, inplace=True)

grouped_WAR_diagnosis_participant=WAR_diagnosis_participant['country'].groupby(WAR_diagnosis_participant['qid_participant'])
g=grouped_WAR_diagnosis_participant.count()
g=g.reset_index()# change the index to column
g.rename(columns={'country': 'has_country'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,g,on='qid_participant',how='left')
WAR_diagnosis_participant=WAR_diagnosis_participant.sort_index(by = ['qid_participant'])
WAR_diagnosis_participant['country_source']=WAR_diagnosis_participant['country_source'].fillna(0)
def function4(a,b,c,d):
    if a!=0 and b==0:
        return d
    else:
        return c

WAR_diagnosis_participant['country_source']=WAR_diagnosis_participant.apply(lambda x: function4(x.has_country,x.country_source,x.country_source,2), axis=1)
WAR_diagnosis_participant['country']=WAR_diagnosis_participant.apply(lambda x: function3(x.country_capital,x.has_country,x.country), axis=1)
WAR_diagnosis_participant.drop(['has_country', 'country_capital'],axis=1, inplace=True)

grouped_WAR_diagnosis_participant=WAR_diagnosis_participant['country'].groupby(WAR_diagnosis_participant['qid_participant'])
g=grouped_WAR_diagnosis_participant.count()
g=g.reset_index()# change the index to column
g.rename(columns={'country': 'has_country'}, inplace=True)
WAR_diagnosis_participant=pd.merge(WAR_diagnosis_participant,g,on='qid_participant',how='left')
WAR_diagnosis_participant['country_source']=WAR_diagnosis_participant['country_source'].fillna(0)
WAR_diagnosis_participant=WAR_diagnosis_participant.sort_index(by = ['qid_participant'])

WAR_diagnosis_participant['country_source']=WAR_diagnosis_participant.apply(lambda x: function4(x.has_country,x.country_source,x.country_source,3), axis=1)
WAR_diagnosis_participant.drop(['has_country'],axis=1, inplace=True)
WAR_diagnosis_participant['country_source']=WAR_diagnosis_participant['country_source'].replace(0,np.nan)# this process embodies the concept of priority: if a qid has got the country information from source 1, then there is no need to got the same information from other sources
WAR_diagnosis_participant.to_csv(main_path+"tmp/WAR_tmp.csv",index=False)


WAR_tmp2=WAR_diagnosis_participant.loc[:,['qid_participant','country','country_source']]
WAR_tmp2['country']=WAR_tmp2['country'].fillna(-1)
WAR_tmp2=WAR_tmp2[WAR_tmp2['country']!=-1]
WAR_tmp2.drop_duplicates(inplace=True)


WAR_diagnosis_participant.drop(['country', 'country_source'],axis=1, inplace=True)
WAR_diagnosis_participant.drop_duplicates(inplace=True)
WAR_country=pd.merge(WAR_diagnosis_participant,WAR_tmp2,on='qid_participant',how='left')
WAR_country.drop(['qid_participant'],axis=1, inplace=True)
WAR_country.drop_duplicates(inplace=True)


grouped_WAR_country=WAR_country['country'].groupby(WAR_country['qid_war'])
g=grouped_WAR_country.count()
g=g.reset_index()# change the index to column
g.rename(columns={'country': 'has_country'}, inplace=True)
WAR_country=pd.merge(WAR_country,g,on='qid_war',how='left')
WAR_country=WAR_country.sort_index(by = ['qid_war'])

WAR_country['country']=WAR_country['country'].fillna(-1)
WAR_country['country_location']=WAR_country['country_location'].fillna(-1)
def function5(a,b):
    if a==-1 and b!=-1:
        return b
    else:
        return a
WAR_country['country']=WAR_country.apply(lambda x: function5(x.country,x.country_location), axis=1)
WAR_country.drop(['has_country', 'country_location'],axis=1, inplace=True)

WAR_country=WAR_country[WAR_country['country']!=-1]
WAR_country['country']=WAR_country['country'].replace(-1,np.nan)

grouped_WAR_country=WAR_country['country'].groupby(WAR_country['qid_war'])
g=grouped_WAR_country.count()
g=g.reset_index()# change the index to column
g.rename(columns={'country': 'has_country'}, inplace=True)
WAR_country=pd.merge(WAR_country,g,on='qid_war',how='left')
WAR_country=WAR_country.sort_index(by = ['qid_war'])
WAR_country['country_source']=WAR_country['country_source'].fillna(0)
WAR_country['country_source']=WAR_country.apply(lambda x: function4(x.has_country,x.country_source,x.country_source,4), axis=1)
WAR_country['country_source']=WAR_country['country_source'].replace(0,np.nan)

#simplify_country
#aggregate_countries
#drop _country_old fine_country has_country
WAR_country['country']=WAR_country['country'].fillna(-1)
WAR_country=WAR_country[WAR_country['country']!=-1]

grouped_WAR_country=WAR_country['country_source'].groupby([WAR_country['qid_war'],WAR_country['country']])
g=grouped_WAR_country.min()
g=g.reset_index()# change the index to column
g.rename(columns={'country_source': 'min_source'}, inplace=True)
WAR_country=pd.merge(WAR_country,g,on=['qid_war','country'],how='left')
WAR_country=WAR_country.sort_index(by = ['qid_war','country'])

WAR_country=WAR_country[WAR_country['country_source']==WAR_country['min_source']]
WAR_country.drop(['min_source'],axis=1, inplace=True)

WAR_country.drop_duplicates(inplace=True)
WAR_country.rename(columns={'qid_war': 'qid'}, inplace=True)
WAR_country=WAR_country.sort_index(by = ['qid'])

WAR_country_wdloc=pd.read_csv(main_path+"tmp/WAR_country_wdloc.csv")
WAR_country_app=WAR_country.append(WAR_country_wdloc,ignore_index=True)
WAR_country_app=WAR_country_app.loc[:,['qid','title','country','country_source','has_country']]


grouped_WAR_country_app=WAR_country_app['qid'].groupby([WAR_country_app['qid'],WAR_country_app['country']])
g=grouped_WAR_country_app.count()
g=pd.DataFrame(g)
g.rename(columns={'qid':'N'}, inplace=True)
g=g.reset_index()# change the index to column
WAR_country_app=pd.merge(WAR_country_app,g,on=['qid','country'],how='left')
WAR_country_app=WAR_country_app.sort_index(by = ['qid','country'])
WAR_country_app['country_source']=WAR_country_app['country_source'].fillna(-1)
WAR_country_app=WAR_country_app.drop(WAR_country_app[(WAR_country_app.N >1)&(WAR_country_app.country_source==-1)].index)
WAR_country_app.loc[WAR_country_app.country_source==-1,'country_source']=6
WAR_country_app['country_source']=WAR_country_app['country_source'].replace(-1,np.nan)
WAR_country_app.drop(['N'],axis=1, inplace=True)
WAR_country_app.to_csv(main_path+"tmp/WAR_country.csv",index=False)


#Bring commanders country to wd data
#A1_maindata=pd.read_stata(main_path+"Data/A1_maindata_21_Jul_2019.dta")
A1_maindata=pd.read_csv(loadin_path+"Data/A1_maindata_21_Jul_2019.csv",encoding="utf-8")
A1_maindata=A1_maindata.loc[:,['subject','country']]
A1_maindata['country']=A1_maindata['country'].fillna(-1)
A1_maindata=A1_maindata[A1_maindata['country']!=-1]
#A1_maindata['subject']=A1_maindata['subject'].str.encode('latin-1').str.decode('utf-8')
#A1_maindata['country']=A1_maindata['country'].str.encode('latin-1').str.decode('utf-8')
A1_maindata.rename(columns={'subject':'commander'}, inplace=True)
A1_maindata['commander']=A1_maindata['commander'].str.lower()
WAR_commander=pd.read_csv(main_path+"tmp/WAR_commander.csv")
WAR_commander=pd.merge(WAR_commander,A1_maindata,on=['commander'],how='left')
WAR_commander.drop(['commander'],axis=1, inplace=True)
WAR_commander['country']=WAR_commander['country'].str.lower()
WAR_commander.drop_duplicates(inplace=True)
WAR_commander.rename(columns={'country':'country_commander'}, inplace=True)

WAR_country=pd.read_csv(main_path+"tmp/WAR_country.csv")
WAR_country.drop(['title'],axis=1, inplace=True)
WAR_commander=pd.merge(WAR_commander,WAR_country,on=['qid'],how='outer')
WAR_commander.drop(['has_country'],axis=1, inplace=True)

grouped_WAR_commander=WAR_commander['country'].groupby(WAR_commander['qid'])
g=grouped_WAR_commander.count()
g=g.reset_index()# change the index to column
g.rename(columns={'country': 'has_country'}, inplace=True)
WAR_commander=pd.merge(WAR_commander,g,on='qid',how='left')
WAR_commander=WAR_commander.sort_index(by = ['qid'])

#use country from commander if country is missing
WAR_commander.loc[WAR_commander.has_country==0,'country']=WAR_commander['country_commander']

WAR_commander.drop(['has_country'],axis=1, inplace=True)
grouped_WAR_commander=WAR_commander['country'].groupby(WAR_commander['qid'])
g=grouped_WAR_commander.count()
g=g.reset_index()# change the index to column
g.rename(columns={'country': 'has_country'}, inplace=True)
WAR_commander=pd.merge(WAR_commander,g,on='qid',how='left')
WAR_commander=WAR_commander.sort_index(by = ['qid'])
WAR_commander['country_source']=WAR_commander['country_source'].fillna(-1)
WAR_commander.loc[(WAR_commander.has_country!=0)&(WAR_commander.country_source==-1),'country_source']=5
WAR_commander['country_source']=WAR_commander['country_source'].replace(-1,np.nan)

WAR_commander['country']=WAR_commander['country'].fillna(-1)
WAR_commander=WAR_commander[WAR_commander['country']!=-1]
WAR_commander.drop(['has_country','country_commander'],axis=1, inplace=True)
WAR_commander.drop_duplicates(inplace=True)



#use country from qid_time_location if country is missing
qid_time_location=pd.read_csv('D:/learning/Arash/war_participants/Fabian_06292019_WAR/Data/qid_time_location.csv')#wikidata
qid_time_location.drop(['Unnamed: 0'],axis=1, inplace=True)
qid_time_location=qid_time_location[ ~ qid_time_location['qidLabel'].str.contains('Cold War')]
qid_time_location=qid_time_location[qid_time_location['qid']!=2310964]    

qid_time_location=qid_time_location.loc[:,['qid','country_ag']]

WAR_commander=pd.merge(WAR_commander,qid_time_location,on='qid',how='outer')

grouped_WAR_commander=WAR_commander['country'].groupby(WAR_commander['qid'])
g=grouped_WAR_commander.count()
g=g.reset_index()# change the index to column
g.rename(columns={'country': 'has_country'}, inplace=True)
WAR_commander=pd.merge(WAR_commander,g,on='qid',how='left')
WAR_commander=WAR_commander.sort_index(by = ['qid'])

WAR_commander.loc[WAR_commander.has_country==0,'country']=WAR_commander['country_ag']

WAR_commander.drop(['has_country'],axis=1, inplace=True)
grouped_WAR_commander=WAR_commander['country'].groupby(WAR_commander['qid'])
g=grouped_WAR_commander.count()
g=g.reset_index()# change the index to column
g.rename(columns={'country': 'has_country'}, inplace=True)
WAR_commander=pd.merge(WAR_commander,g,on='qid',how='left')
WAR_commander=WAR_commander.sort_index(by = ['qid'])
WAR_commander['country_source']=WAR_commander['country_source'].fillna(-1)
WAR_commander.loc[(WAR_commander.has_country!=0)&(WAR_commander.country_source==-1),'country_source']=7
WAR_commander['country_source']=WAR_commander['country_source'].replace(-1,np.nan)

WAR_commander['country']=WAR_commander['country'].fillna(-1)
WAR_commander=WAR_commander[WAR_commander['country']!=-1]
WAR_commander.drop(['has_country','country_ag'],axis=1, inplace=True)
WAR_commander.drop_duplicates(inplace=True)

mapContinent=pd.read_stata(mapContinent_path)
mapContinent['location']=mapContinent['location'].str.encode('latin-1').str.decode('utf-8')
mapContinent['continent']=mapContinent['continent'].str.encode('latin-1').str.decode('utf-8')
mapContinent.rename(columns={'location': 'country'}, inplace=True)
mapContinent['country']=mapContinent['country'].str.lower()
mapContinent['country']=mapContinent['country'].str.strip()
WAR_commander['country']=WAR_commander['country'].str.lower()
WAR_commander['country']=WAR_commander['country'].str.strip()

WAR_commander=pd.merge(WAR_commander,mapContinent,on='country',how='left')
WAR_commander.loc[WAR_commander.country=='united_kingdom','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='montenegro','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='romania','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='bulgaria','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='australia','continent']='Oceania'
WAR_commander.loc[WAR_commander.country=='czechoslovakia','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='netherlands','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='colombia','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='isle_of_man','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='czech_republic','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='sweden','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='albania','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='philippines','continent']='Asia'
WAR_commander.loc[WAR_commander.country=='venezuela','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='brazil','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='argentina','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='congo','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='croatia','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='finland','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='vatican_city','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='uruguay','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='paraguay','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='peru','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='bolivia','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='suriname','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='guinea','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='comoros','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='ecuador','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='côte_d’ivoire','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='arabia','continent']='Asia'
WAR_commander.loc[WAR_commander.country=='bosnia_and_herzegovina','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='uk','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='timor-leste','continent']='Asia'
WAR_commander.loc[WAR_commander.country=='serbia_and_montenegro','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='macedonia','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='seychelles','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='central_african_republic','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='chad','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='solomon_islands','continent']='Oceania'
WAR_commander.loc[WAR_commander.country=='gambia','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='åland_islands','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='maldives','continent']='Asia'
WAR_commander.loc[WAR_commander.country=='belgiumluxemburg','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='us','continent']='North_America'

WAR_commander.loc[WAR_commander.country=='caucasia','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='falkland_islands','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='westafrica','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='guyana','continent']='South_America'
WAR_commander.loc[WAR_commander.country=='eastafrica','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='russian_turkestan','continent']='Asia'
WAR_commander.loc[WAR_commander.country=='syrialebanon','continent']='Asia'
WAR_commander.loc[WAR_commander.country=='palestinian','continent']='Asia'
WAR_commander.loc[WAR_commander.country=='eritreaandethiopia','continent']='Africa'
WAR_commander.loc[WAR_commander.country=='ancient_greece','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='holy_roman_empire','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='ru/северо-восточная_русь','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='republic_of_kosova','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='tonkin','continent']='Asia'
WAR_commander.loc[WAR_commander.country=='archduchy_of_austria','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='ruthenia','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='pryazovia','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='kingdom_of_etruria','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='yishuv','continent']='Asia'
WAR_commander.loc[WAR_commander.country=='cochinchina','continent']='Asia'
WAR_commander.loc[WAR_commander.country=='terra_mariana','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='protectorate_of_bohemia_and_moravia','continent']='Europe'
WAR_commander.loc[WAR_commander.country=='kabardino-balkaria','continent']='Europe'




WAR_commander=WAR_commander[WAR_commander['country']!='music_of_the_united_states']

WAR_commander=WAR_commander.sort_index(by = ['qid'])
WAR_commander['continent']=WAR_commander['continent'].replace(0,np.nan)
WAR_commander.to_csv(main_path+"output/WWdata_qid_country_continent.csv", index=False)



WAR_commander['continent']=WAR_commander['continent'].fillna(0)

nocontinent=WAR_commander[WAR_commander['continent']==0]
nocontinent=nocontinent.loc[:,['country','continent']]
nocontinent.drop_duplicates(inplace=True)

sa=mapContinent[mapContinent['continent']=='North_America']
nocontinent['qid']=nocontinent['qid'].astype(int)
