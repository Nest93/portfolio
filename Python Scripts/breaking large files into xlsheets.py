# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 18:21:01 2021

@author: SamoladasN
"""

import pandas as pd

df=pd.read_csv(r'C:\Users\samoladasn\OneDrive\Other Files\All bundles Mar20-Aug21.csv')


excel_row_limit=1048574


if df.shape[0]/excel_row_limit > 1:
    
    print("we will need {} sheets".format(round(df.shape[0]/excel_row_limit,2)))
        
    list_sheets=[]
    
    sh1=df[0:excel_row_limit]
    
    list_sheets.append(sh1)
    current_row=sh1.shape[0]
    
    rows_left=df.shape[0]-sh1.shape[0]
    
    while rows_left > 0:
        
        if rows_left > excel_row_limit:
            
            next_sh=df[current_row:current_row + excel_row_limit]
            list_sheets.append(next_sh)
            
            current_row=next_sh.index[-1]
            rows_left=rows_left-next_sh.shape[0]
        
        else:
            
            next_sh=df[current_row:]
            list_sheets.append(next_sh)
            
            current_row=next_sh.index[-1]
            rows_left=rows_left-next_sh.shape[0]
            
    print(list_sheets)    

else:

    print('\nFile has less rows than the excel limit...It can be opened using excel!\n')      
        


writer=pd.ExcelWriter(r'C:\Users\samoladasn\Downloads\Allbundles_mar20_aug21.xlsx', engine='xlsxwriter')
        
for n,item in enumerate(list_sheets):

    list_sheets[n].to_excel(writer,index=False,sheet_name='Sheet {}'.format(n+1))

writer.save()
