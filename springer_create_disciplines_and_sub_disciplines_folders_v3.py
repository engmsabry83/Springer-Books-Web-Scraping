# -*- coding: utf-8 -*-
"""
Created on Sun May  3 12:40:11 2020

@author: engMa
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
import os

#PLEASE CONSIDER CHANGING THE POSTGRES PASSWORD
engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

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

def f_clean_folder_name(name):
    name=name.replace('/','-')
    return name

def f_get_books_urls(url):
    df_sub_discipline_books=pd.DataFrame(columns=['book_title','book_url'])
    #url='https://link.springer.com/search?facet-content-type=%22Book%22&just-selected-from-overlay=facet-sub-discipline&facet-discipline=%22Biomedicine%22&showAll=false&just-selected-from-overlay-value=%22Anatomy%22&facet-sub-discipline=%22Anatomy%22'
    page_no=1
    while True:
        books_url=url.replace('/search?','/search/page/'+str(page_no)+'?')+'&showAll=false'
        res0=f_get_url(books_url)
        page1=res0.content
        soup1=BeautifulSoup(page1, 'lxml')
        print("\tScanning page: ",page_no)
        try:
            page_books=soup1.find_all('div',{'class':'text'})
        except:
            break
        if len(page_books)==0:
            break
        for b in range(len(page_books)):
            book_title=page_books[b].a.text
            book_url=page_books[b].a.get('href')
            df_sub_discipline_books.loc[len(df_sub_discipline_books)]=[book_title,book_url]
        if len(df_sub_discipline_books)>len(df_sub_discipline_books.drop_duplicates()):
            df_sub_discipline_books=df_sub_discipline_books.drop_duplicates()
            break
        page_no+=1
        time.sleep(5)
    return df_sub_discipline_books
            
            

def f_create_discipline_directories_tree():  
    sql_str='SELECT distinct discipline_name, sub_discipline_name,sub_discipline_url FROM "Springer".springer_sub_disciplines order by 1,2'
    df= pd.read_sql_query(sql_str,con=engine)
    
    for i in range(0,len(df)):
        discipline_name=df['discipline_name'][i]
        discipline_name=f_clean_folder_name(discipline_name)
        print("Discipline: ",discipline_name)
        sub_discipline_name=df['sub_discipline_name'][i]
        sub_discipline_name=f_clean_folder_name(sub_discipline_name)
        print("\t Sub-Discipline: ",sub_discipline_name)
        discipline_path=os.getcwd()+"\\Springer\\"+discipline_name
        sub_discipline_path=discipline_path+"\\"+sub_discipline_name
        if not os.path.isdir(discipline_path):
            os.makedirs(discipline_path)
        if not os.path.isdir(sub_discipline_path):
            os.makedirs(sub_discipline_path)

def f_get_disciplines_and_sub_disciplines_books_urls():
    sql_str='SELECT distinct discipline_name, sub_discipline_name,sub_discipline_url FROM "Springer".springer_sub_disciplines order by 1,2'
    df= pd.read_sql_query(sql_str,con=engine)
    df_len=len(df)
    for i in range(df_len):
        print("Scanning sub-discipline "+str(i+1)+" out of "+ str(df_len))
        pdf=f_get_books_urls(df['sub_discipline_url'][i])
        print("\tTotal Books found= "+str(len(pdf)))
        pdf['discipline_name']=df['discipline_name'][i]
        pdf['sub_discipline_name']=df['sub_discipline_name'][i]
        pdf['collection_time']=dt.datetime.now()
        pdf.to_sql('springer_sub_disciplines_books', engine,schema='Springer',if_exists='append', index=False,chunksize=1000)

def f_download_books():  
    sql_str='SELECT book_title, book_url, discipline_name, sub_discipline_name FROM "Springer".v_springer_sub_disciplines_books order by 3,4'
    df= pd.read_sql_query(sql_str,con=engine)
    df_len=len(df)
    for i in range(0,df_len):
        print("Downloading book "+str(i+1)+ " out of " + str(df_len))
        print("\t >>> "+df['discipline_name'][i]+"\\"+f_clean_folder_name(df['sub_discipline_name'][i])+"\\"+df['book_title'][i]+".pdf")
        book_download_url='https://link.springer.com/content/pdf/'+df['book_url'][i].split('book/')[1].replace('/','%2F')+'.pdf'
        book_response=f_get_url(book_download_url)
        pdf_path= os.getcwd()+'\\Springer\\'+df['discipline_name'][i]+'\\'+f_clean_folder_name(df['sub_discipline_name'][i])+'\\'+df['book_title'][i]#+'.pdf'
        file_no=0
        while True:
            if os.path.exists(pdf_path+".pdf"):
                print("\tFound a book with the same name, will rename it")
                file_no+=1
                pdf_path=pdf_path+"_"+str(file_no)
                continue
            else:
                break
        with open(pdf_path+".pdf", 'wb') as f:
            f.write(book_response.content)
        time.sleep(5)
        
        
def main():
    start_time=dt.datetime.now()
    #f_get_disciplines_and_sub_disciplines_books_urls()
    #f_create_discipline_directories_tree()
    f_download_books()
    end_time=dt.datetime.now()
    print("Started at: "+str(start_time))
    print("Finished at at: "+str(end_time))
    print("Total time taken: "+str(end_time-start_time))
    #f_get_disciplines_and_sub_disciplines_books_urls()
        
if __name__=='__main__':
    main()
