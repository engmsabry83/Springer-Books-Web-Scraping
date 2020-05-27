# -*- coding: utf-8 -*-
"""
Created on Sat May  2 14:57:22 2020

@author: Mahmoud Sabri
@email: Eng.MahmoudSabry@gmail.com

"""
import requests as req
import pandas as pd
import time
from sqlalchemy import create_engine
import datetime as dt
from bs4 import BeautifulSoup
from datetime import timedelta, date
import re
import pandasql as pdsql
import platform

url_base='https://link.springer.com'

def f_get_url(url,timeout=20):
    while True:
        try:
            req_response = req.get(url,timeout=timeout,allow_redirects=False)
            break
        except:
            print("Connection refused by the server..")
            print("Let me sleep for 1 minute, then retry")
            time.sleep(60)
            continue
    return req_response


def f_get_disciplines():
    
    df_disciplines=pd.DataFrame(columns=['discipline_name','discipline_url','collection_time'])
    url='https://link.springer.com'
    res0=f_get_url(url)
    page0=res0.content
    soup0=BeautifulSoup(page0, 'lxml')
    disciplines=soup0.find_all('ol',{'class':'disciplines'})[0].find_all('a')
    
    for rec in range(len(disciplines)):
        discipline_name=disciplines[rec].text
        discipline_url=disciplines[rec].get('href')
        print(discipline_name)
        discipline_url=url+discipline_url
        print(discipline_url)
        ct=dt.datetime.now()
        df_disciplines.loc[len(df_disciplines)]=[discipline_name,discipline_url,ct]
    
    print("\nAll Disciplines found= ",len(df_disciplines))
    return df_disciplines


def f_get_sub_disciplines(discipline_url,discipline_name):
    df_sub_disciplines=pd.DataFrame(columns=['discipline_name','sub_discipline_name','sub_discipline_url'])
    #url='https://link.springer.com/search/facetexpanded/sub-discipline?facet-discipline=%22Computer+Science%22&showAll=false&facet-content-type=%22Book%22'
    url=discipline_url.replace('https://link.springer.com/search?facet-discipline=','https://link.springer.com/search/facetexpanded/sub-discipline?facet-discipline=')+'&showAll=false&facet-content-type=%22Book%22'
    page_no=1
    while True:
        print("\tScanning Sub-disciplines page no: "+str(page_no))
        res1=f_get_url(url+'&page='+str(page_no))
        page1=res1.content
        soup1=BeautifulSoup(page1,'lxml')
        try:
            sub_disciplines=soup1.find_all('ol')[0].find_all('li')
        except:
            break
        for s in range(len(sub_disciplines)):
            sub_discipline_name=sub_disciplines[s].find_all('span',{'class':'facet-title'})[0].text
            sub_discipline_url=sub_disciplines[s].a.get('href')
            sub_discipline_url=url_base+sub_discipline_url
            df_sub_disciplines.loc[len(df_sub_disciplines)]=[discipline_name,sub_discipline_name,sub_discipline_url]
        if len(df_sub_disciplines)>len(df_sub_disciplines.drop_duplicates()):
            df_sub_disciplines=df_sub_disciplines.drop_duplicates()
            break
        else:
            page_no+=1
            time.sleep(4)
    print("\tEnd of Scanning sub-disciplines!!")
    df_sub_disciplines['collection_time']=dt.datetime.now()        
    #print("\tNo of Disciplinees in page "+str(page_no)+" = "+str(len(sub_disciplines)))
    return df_sub_disciplines
        

    

        
def main():
    #PLEASE CONSIDER CHANGING THE POSTGRES PASSWORD
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
    df_disciplines=f_get_disciplines()
    print("Total Disciplines collected = ",len(df_disciplines))
    df_disciplines.to_sql('springer_disciplines', engine,schema='Springer',if_exists='append', index=False,chunksize=1000)
    total_disciplines=len(df_disciplines)
    for d in range(total_disciplines):
        print("\nCollecting ",d+1," out of ",total_disciplines," Disciplines >>> ",df_disciplines['discipline_name'][d])
        try:
            df_sub_disciplines=f_get_sub_disciplines(df_disciplines['discipline_url'][d],df_disciplines['discipline_name'][d])
        except:
            print("\tCould'nt find Books under Discipline: ",df_disciplines['discipline_name'][d]," !!")
            continue
        print("\tTotal Sub-Disciplines collected = ",len(df_sub_disciplines))
        df_sub_disciplines.to_sql('springer_sub_disciplines', engine,schema='Springer',if_exists='append', index=False,chunksize=1000)

        
    
if __name__=='__main__':
    main()
