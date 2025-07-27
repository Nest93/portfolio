# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 21:08:27 2023

@author: Nestoras
"""


import pandas as pd 
import sqlite3,os

import_path=r'C:\Users\samoladasn\OneDrive\The Prepay team (N&D)\Training_Manuals_Forms\Scripts'
conn=sqlite3.connect(r'C:\Users\samoladasn\OneDrive\Scripts\SQLite\Prepaid DB.db')


df_counter=1

for file in os.listdir(import_path):
    
    print(file)
    data=pd.read_excel(import_path+'\\'+ file)
    
    if df_counter == 1: 
                            
        all_data = pd.DataFrame (columns = data.columns)
    
    
    
    all_data = pd.concat ( [all_data,data] ,ignore_index=True)        
            
    df_counter+=1 
    
def first_rechargers_table():
    
      c=conn.cursor()
      c.execute(
         
          """ 
          CREATE TABLE first_rechargers
          
          (
              
              Month text,
              Date text,
              MSISDN text,
              Tariff_Plan_Category text,
              Total_nominal_Value_of_recharges_with_Tax real,
              POS_code text,
              XPostpay_flag text,
              Dealer text,
              Student_Flag text,
              Dlrcode text,
              Dealer_Name text,
              Region text,
              Channel_Name text
              Subchannel text,
              PRIMARY KEY("Month", "MSISDN")
              
              )
      
      """)
      
      conn.commit()

first_rechargers_table()

all_data.to_sql('first_rechargers',con=conn,if_exists='append',index=False)


def insert_new_data(filepath):
    
    first_rech_brains=pd.read_excel(filepath,usecols=['Month','Day (yyyy-mm-dd)','MSISDN','Tariff Plan Category','Total nominal Value of recharges (with Tax)',
                                                      'Dealer (on Connection)','POS code (on Connection)','XPostpay flag','Channel Name'])
    
    
    
    first_rech_brains.rename(columns={'Day (yyyy-mm-dd)':'Day_yyyy_mm_dd',
                                      'Channel Name':'Subchannel',
                                      'Dealer (on Connection)':'Shop_Code',
                                      'POS code (on Connection)':'POS_Code',
                                      'XPostpay flag':'XPostpay_flag',
                                      'Total nominal Value of recharges (with Tax)':'Total_nominal_Value_of_recharges_with_Tax',
                                      'Tariff Plan Category':'Tariff_Plan_Category'},inplace=True)
    
    first_rech_brains.to_sql('first_rechargers',con=conn,if_exists='append',index=False)
    
    conn.close()

insert_new_data('')



#--- Show database info ---- 

def db_info(): 
    
    print ('\nTable names\n', pd.read_sql("""
                
                SELECT name
                FROM sqlite_schema
                WHERE type='table' and name NOT LIKE 'sqlite_%';
                
                """,conn)
            )
           
    print  ( '\nColumns\n',pd.read_sql("""
                                     
                                     PRAGMA table_info(projects)
                                     
                                     """,conn)
        
            )


db_info()
