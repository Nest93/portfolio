# -*- coding: utf-8 -*-
"""
Created on Sat Oct  1 13:09:27 2022

@author: nsamo


Using the open method, since pandas read_csv and read_excel do not work because of the corrupted file from SAP.

Result is a txt file ready to be used as input in the SPSS stream.

"""


import os
import pandas as pd


def reading_data(filepath):
    
    with open (filepath,"r") as file:
        
        file_content = file.readlines()
    
    return file_content


    
def data_transformations(file_content):
    
    df = pd.DataFrame({"col":file_content}) # loading everything into a single column
    
    df1=df["col"].str.split("\t",expand=True) # split evetyhing into columns 
    
    df1=df1.drop(df1.index[0:18]) # drop the 1st 17 lines 
    df1.reset_index(drop=True,inplace=True) 
    
    df1.drop(df1[(df1[0].isin(["Date:","User:","ALV Lines:","Correction Lines:"]))].index,inplace=True) #droping these lines as they dont contain useful info
    df1.reset_index(drop=True,inplace=True)
    
    df1=df1.drop([0],axis=1) # droping the 1st column
    
    
    df1.columns=df1.iloc[0] #making the 1st row column headers
    
    df1=df1.drop(df1.index[0:2]) # remove the 2nd row that contains the headers
    
    df1.dropna(how='all',inplace=True) #remove the lines that have all values NAs
    
    df1.drop(df1[df1["DT"]=="DT"].index,inplace=True) # the headers in each table begins with "DT", so we remove evey occurence of DT (AND THEREFORE THE ENTIRE LINE)
    
    df1.drop(columns=[""],inplace=True) #dropping empty columns
    
    df1.reset_index(drop=True,inplace=True) 
    
    df1.set_axis(['DT', 'ΗμερΚαταχ.', 'Λογ.Σύμβ.', 'Ποσό', 'Εκκαθάριση','ΗμερΚατΕκκ', 'ΠοσΕκ'], axis='columns', inplace=True) #fixing column names
    
    df1['Ποσό']=df1['Ποσό'].str.replace("\n","")
    df1['ΠοσΕκ']=df1['ΠοσΕκ'].str.replace("\n","")
    
    df1['Ποσό']=df1['Ποσό'].str.strip()
    df1['ΠοσΕκ']=df1['ΠοσΕκ'].str.strip()
    
    
    df1['Ποσό']=df1['Ποσό'].str.replace('.',"")
    df1['ΠοσΕκ']=df1['ΠοσΕκ'].str.replace('.',"")
    
    
    df1['Ποσό']=df1['Ποσό'].str.replace(',','.')
    df1['ΠοσΕκ']=df1['ΠοσΕκ'].str.replace(',','.')
    
    df1['Ποσό']=df1['Ποσό'].astype(float)
    df1['ΠοσΕκ']=df1['ΠοσΕκ'].astype(float)


    df1['Ποσό']=df1['Ποσό'].fillna(0)
    df1['ΠοσΕκ']=df1['ΠοσΕκ'].fillna(0)
    
    return df1

def data_concat (import_path):

    df_counter=1
    
    global all_data # so that can be used outside of this func 
    
    for file in os.listdir(import_path): 
        
            if df_counter == 1 : 
                
                print('\nReading Files from file path: \n')
        
            print(file) 
            
            # calling the two functions 
            data = reading_data (import_path + '\\' + file)
            data = data_transformations (data)
            
            
            # creating a dataframe to concatenate all outputs
            
            # df_counter=1 means it is the 1st time we go into the loop,
            # so we take the headers from the 1st output.
            
            if df_counter == 1: 
                            
                all_data = pd.DataFrame (columns = data.columns)
    
    
    
            all_data = pd.concat ( [all_data,data] ,ignore_index=True)        
            
            df_counter+=1 

    
    return  all_data




def SPSS_output ():
    
        
    # the pvt
    all_data_pvt=pd.pivot_table(all_data,values='ΠοσΕκ', aggfunc='sum',index=['Λογ.Σύμβ.','ΗμερΚαταχ.','DT'])
    all_data_pvt=all_data_pvt.reset_index() # making the multi-index two columns so that it is easier to use
    
    print('\n> All Data pvt: \n')
    print(all_data_pvt)
    
    
    # the SPSS df
    
    spss_df=all_data_pvt[all_data_pvt['DT'].isin(['DC','DB','RC','RD'])]
    
    spss_df.rename(columns={all_data_pvt.columns[0]:'OU_NUM',
                            all_data_pvt.columns[1]:'Date',
                            all_data_pvt.columns[2]:'DT',
                            all_data_pvt.columns[3]:'Total'},inplace=True)
    
    
    spss_df['OU_NUM;Date;DT;Total']=spss_df[['OU_NUM','Date','DT','Total']].astype(str).agg(';'.join,axis=1)
    spss_df.reset_index()
    
    
    print('\n> Spss df: \n')
    print(spss_df)
    
    spss_df=spss_df['OU_NUM;Date;DT;Total']
    
    spss_df.to_csv( import_path + '\\' + 'SAP EXPORTS.txt',index=False)
    
    
    return spss_df



import_path = r'C:\Users\samoladasn\OneDrive - Vodafone Group\Desktop\Nestoras Work\Commissions & Penalties\Direct Debits\2023\Mar23\SAP EXPORTS'

df = data_concat(import_path)

SPSS_output()





