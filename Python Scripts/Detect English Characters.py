# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 17:50:41 2021

@author: nsamo
"""

##############################################################################
#                       Notes
# Code is ready!


 
#              Typical Troubleshooting 

#1 : TypeError: descriptor 'isascii' for 'str' objects doesn't apply to a 'float' object
#Solution : Propably there are NaNs in the column you are checking or numbers



##############################################################################


import os 
import pandas as pd

def check_if_English(df,col_name_tocheck):
    '''
    name of the column in QUOTES
    '''
    
    df['IsEnglish']=df[col_name_tocheck].map(str.isascii) #ASCII returns False if a character is non-English
    
    print('\n---- Dataset ---\n',
          'total number of rows:'+str(df.shape[0]),\
            '\n'+ str(df.groupby('IsEnglish')[col_name_tocheck].count()))
        
    if len(df['IsEnglish'][df['IsEnglish']==False].value_counts())==0: 
    
          print('\nNo errors found')   
       
    else:
          print('\nInvalid entries found in the following codes:\n','\n', df[[col_name_tocheck,'IsEnglish']][df['IsEnglish']==False])
        
    exporting_order = input('Proceed with export[y/n] ? ')

    if exporting_order in ['yes', 'YES', 'Yes','Y','y']:
        'exporting data with the IsEnglish flag'
        file_name=input('Provide Filename: ')
        
        data.to_excel(file_name +'.xlsx',index=False)
        print ('\nFile exported at: ','\n' + os.getcwd())
        
    else: 
        print('--- Export Cancelled by the user ---')
    


#read data
data=pd.read_excel(r'C:\Users\samoladasn\OneDrive\Other Files\Hunters Capacity\2021\nov21\Hunters November21 updated.xlsx') # specifying the sheet to read from


## run function here
check_if_English(data,'Code')




