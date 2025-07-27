# -*- coding: utf-8 -*-
"""
Created on Mon Mar  8 19:15:14 2021

@author: SamoladasN

Notes:
    
-- Be careful of the encoding of the fd prepaid report. You must specify that it is UTF-16 , 
otherwise pandas will try to decode it as UTF-8 and it will raise error. Also note that the 
separator is tab (\t), you have to specify that!


# Issue # 1: Τhe join returns some DLRs from jup codes like 040, 1000, 3000 that VLOOKUP could not match --> find solution?

# Issue # 2: ||SOLVED||  Duplicates in MSISDNs ---> maybe sort them first by last updated column and then keep the first?

# Issue # 3: ||SOLVED|| : Case senstivity, cannot match the usernames in the Youth bo with the fd prepaid_report

LAST RUN stats: Time Elapsed:  129.89 sec

"""

import pandas as pd
import numpy as np
import time
import warnings

warnings.filterwarnings("ignore")

# Step 1: Change the input directory

input_dir  = r'C:\Users\samoladasn\OneDrive - Vodafone Group\Desktop\Nestoras Work\Commissions & Penalties\Full Digital\2023\Jan23\Prepay' 


print('\nReading input files...')

t0=time.time()

#step 2: Check the directories of the input files 

fd_prepaid_report      = pd.read_csv (input_dir+'\\Prepay - Full Digital report.csv',sep=";")
# fd_prepaid_report      = pd.read_excel  (input_dir+'\\Prepay - Full Digital report.xlsx',encoding = 'UTF-16',sep='\t'
dealer_list            = pd.read_excel(r'C:\Users\samoladasn\OneDrive - Vodafone Group\The Prepay team (N&D)\Other Files\Dealer List\Siebel Dealer List 012023.xlsx', sheet_name='dl')
ha_bundles             = pd.read_excel(input_dir+'\\monthly_stats_1.xlsx', sheet_name='fd')
youth_bo               = pd.read_excel(r'C:\Users\samoladasn\OneDrive - Vodafone Group\Desktop\Nestoras Work\Commissions & Penalties\Full Digital\aiosusernames 2022 youth.xlsx',sheet_name='youth backoffice',usecols=[1])
vsa_bo                 = pd.read_excel(input_dir+'\\Hunters Mobile AIOS 2023 Jan.xlsx')
last_month_fd_score    = pd.read_excel(r'C:\Users\samoladasn\OneDrive - Vodafone Group\Desktop\Nestoras Work\Commissions & Penalties\Full Digital\2022\Dec22\Prepay\FD Prepay Penalty Dec22 per MSISDN.xlsx', sheet_name='Fran-VE', usecols=[0,6])
last_month_fd_score_ws = pd.read_excel(r'C:\Users\samoladasn\OneDrive - Vodafone Group\Desktop\Nestoras Work\Commissions & Penalties\Full Digital\2022\Dec22\Prepay\FD Prepay Penalty Dec22 per MSISDN.xlsx', sheet_name='Wholesale', usecols=[0,5])


fd_prepaid_report=fd_prepaid_report[fd_prepaid_report['Dealer Code'].str.endswith("DR")==False]
fd_prepaid_report.reset_index(drop=True,inplace=True)


def check_if_English(df,col_name_tocheck):
    
    '''
    name of the column in QUOTES
    '''
    df[col_name_tocheck]=df[col_name_tocheck].astype('str')
    df['IsEnglish']=df[col_name_tocheck].map(str.isascii) #ASCII returns False if a character is non-English
    
    print('\n---- Dataset ---\n',
          'total number of rows:'+str(df.shape[0]),\
            '\n'+ str(df.groupby('IsEnglish')[col_name_tocheck].count()))
        
    if len(df['IsEnglish'][df['IsEnglish']==False].value_counts())==0: 
    
          print('\nNo language errors found')   
       
    else:
          print('\nInvalid entries found in the following codes:\n','\n', df[[col_name_tocheck,'IsEnglish']][df['IsEnglish']==False])
            

def FD_penalty_calc (fd_prepaid_report,dealer_list,ha_bundles,youth_bo,vsa_bo,last_month_fd_score):
    
    # Performing initial language checks in the VSA_BO and Youth_bo dfs for english!
    
    print('\nVerifying english language...\n')
    
    check_if_English(vsa_bo,'AIOS Usernames')
    check_if_English(vsa_bo,'AIOS VF Root Usernames')
    check_if_English(youth_bo,'AIOS VF Root Usernames')
    
    df_list=[vsa_bo,youth_bo]
    
    for df in df_list:
        
        if (df['IsEnglish']==False).any():
            
               raise ValueError('\nThere is something wrong with the language in one of the datasets\nTerminating execution... Please fix the language issue and run again!')


    # removing whitespace from the VSA & youth bo columns 
    
    vsa_bo['AIOS Usernames']         = vsa_bo['AIOS Usernames'].str.strip()
    vsa_bo['AIOS VF Root Usernames'] = vsa_bo['AIOS VF Root Usernames'].str.strip()
    youth_bo['VSA']                  = youth_bo['AIOS VF Root Usernames'].str.strip()
    

    # Checking for duplicated entries in MSISDN column
    
    if fd_prepaid_report.duplicated('MSISDN').any():
    
        print('\nDuplicated Entries in MSISDN: ',fd_prepaid_report.duplicated('MSISDN').any(),'\n Removing Duplicates...')
        
        
        # Sorting the dataset from the latest date to the earliest (according to the last updated column)
        fd_prepaid_report=fd_prepaid_report.sort_values('Last Updated Date', ascending=False).reset_index(drop=True)
    
    
        # Having sorted the dataset we keep only the first occurence of the duplicate entries. 
        fd_prepaid_report.drop_duplicates(subset=['MSISDN'], keep='first', inplace=True, ignore_index=True)
    
    else:
        print('\nNo duplicated entries in MSISDN\n')
    
    
    #Step 1: Bring in the DLR to the prepaid report from dealer list, the following emulates a Vlookup in excel
    
    fd_prepaid_report['DLR Code']=pd.merge(fd_prepaid_report['Dealer Code'], dealer_list[['Jup Code','Dlrcode']],how='left', left_on='Dealer Code', right_on='Jup Code')['Dlrcode']
    fd_prepaid_report.dropna(axis=0,subset=['DLR Code'],inplace=True)
    fd_prepaid_report.reset_index(drop=True,inplace=True)
    
    
    ### Creating the template file
    
    # Step 2: Bring POS code, Dealer Name from dealer list in the new dataframe
    
    fd_prepaid_pen = fd_prepaid_report[['Order Date','MSISDN','Dealer Code','Created by','Full Digital','DLR Code']]
    
    excl_dealer_codes=~fd_prepaid_pen['Dealer Code'].isin(["DF620", # we want to exclude these deal numbers
                                                           "HOL016",
                                                           "DF620",
                                                           "DM7",
                                                           "040"])
    
    fd_prepaid_pen=fd_prepaid_pen[excl_dealer_codes]
    fd_prepaid_pen.reset_index(drop=True,inplace=True)
    
    fd_prepaid_pen.loc[:,'POS code']=pd.merge(fd_prepaid_pen['Dealer Code'], dealer_list[['Jup Code','Poscode']],how='left', left_on='Dealer Code', right_on='Jup Code').loc[:,'Poscode']
    fd_prepaid_pen.loc[:,'Dealer name'] = pd.merge(fd_prepaid_pen['Dealer Code'], dealer_list[['Jup Code','Dealer Name']],how='left', left_on='Dealer Code', right_on='Jup Code').loc[:,'Dealer Name']
    fd_prepaid_pen.loc[:,'Channel'] = pd.merge(fd_prepaid_pen['Dealer Code'], dealer_list[['Jup Code','Channel Name']],how='left', left_on='Dealer Code', right_on='Jup Code').loc[:,'Channel Name']
    
    
    # Step 3: Create the Hunter App flag
    
    ha_bundles.drop_duplicates(subset=['MSISDN'],keep='first',inplace=True,ignore_index=True)
    
    d=pd.merge(fd_prepaid_pen,ha_bundles['MSISDN'],how='left',on='MSISDN',indicator='Hunter App')
    
    from_ha=d['Hunter App']=='both'
    not_from_ha=d['Hunter App']!='both'
    
    d.loc[from_ha,'HuBOter app flag']='Y'
    d.loc[not_from_ha,'HuBOter app flag']='N'
    
    # Step 4: Check Back Office
    
    m=input('Checking Back Office VSA... Which month I should look for(eg. ΙΑΝΟΥΑΡΙΟΣ):')
    vsa_bo=vsa_bo[['Name','AIOS Usernames','AIOS VF Root Usernames',m]][vsa_bo[m]=='V']
    vsa_bo.reset_index(drop=True,inplace=True)
    
    d['Back Office VSA']=d.loc[:,'Created by'].isin(vsa_bo['AIOS Usernames'].str.upper()) | d.loc[:,'Created by'].isin(vsa_bo['AIOS VF Root Usernames'].str.upper())
    d['Back Office VSA']=pd.Categorical(values=d['Back Office VSA'],categories={False:'FALSE', True:'TRUE'})
    
    # Step 5: Check Youth bo
    
    youth_bo.dropna(axis=0,subset=['AIOS VF Root Usernames'],inplace=True)
    youth_bo.reset_index(drop=True,inplace=True)
    
    all_the_false=d['Back Office VSA']== False
    
    youth_flag=d['Created by'].str.upper().isin(youth_bo['AIOS VF Root Usernames'].str.upper())
    
    d['Back Office VSA'] = d['Back Office VSA'].cat.add_categories('YOUTH')
    
    d.loc[all_the_false & youth_flag,'Back Office VSA']='YOUTH'
    
    # Step 6: Changing to BO in column HuBOter Flag whatever has value true or youth in column Back Office VSA
    
    for_BO=(d['HuBOter app flag']=='N') & ((d['Back Office VSA']=='YOUTH') | (d['Back Office VSA']==True)) #despite that we
    #turned the Back Office VSA to categorical the value are still boolean that is why is == True and not =='TRUE' 
    
    d.loc[for_BO,'HuBOter app flag']='BO'
    
    # Step 7: Bringing in the last month's FD Score
    
    d['Ποσοστό επίτευξης FD προηγούμενου μήνα']=pd.merge(d['DLR Code'],last_month_fd_score,how='left',on='DLR Code')['Ποσοστό επίτευξης Full Digital']
    
    
    # Step 8: Setting the penalty 
    
    penalty_conditions= (d['Channel'].isin(['VODAFONE SHOPS','EXCLUSIVE DEALERS'])) &\
                        (d['Full Digital']=='N')                                   &\
                        (d['HuBOter app flag']=='N')
    
    
    
    def penalty(FD_Score): #defining a function who runs the check 
        
        if FD_Score < 0.8:
            return -8
        else: 
            return -4
    
    d['ΠΟΣΟ ΠΑΡΑΚΡΑΤΗΣΗΣ']=d.loc[penalty_conditions,:].apply(lambda row: penalty(row['Ποσοστό επίτευξης FD προηγούμενου μήνα']),axis=1)
    
    # Step 9: Creating the rest of the columns to match the template
    
    d['Bundle_Name']=np.nan
    d['C2_Prepay subscriber Attribute']= np.nan
    d['D5_1st Recharge Date']=np.nan
    d['D4_ΗΜΕΡΟΜ.ΤΑΥΤΟΠΟΙΗΣΗΣ']=np.nan
    
    d.rename(columns={'Order Date':'Month', 'Dealer Code':'Deal Number', 'Created by':'VSA creator'}, inplace=True)
    d['Month']=(d['Month'].astype('datetime64').dt.date).astype('datetime64')
        
    
    
    fd_prepaid_pen=d[['Month','MSISDN','POS code','Dealer name','Bundle_Name','HuBOter app flag',\
                      'Full Digital','Channel','DLR Code','C2_Prepay subscriber Attribute',\
                      'D5_1st Recharge Date','D4_ΗΜΕΡΟΜ.ΤΑΥΤΟΠΟΙΗΣΗΣ','Ποσοστό επίτευξης FD προηγούμενου μήνα',\
                      'ΠΟΣΟ ΠΑΡΑΚΡΑΤΗΣΗΣ','VSA creator','Back Office VSA','Deal Number']]
    
        
        

    
    #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #                         P-I-V-O-T-S
    
    #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
     
    # keeping only the usuful columns from dealer list, and also keeping the unique values from the DLR col to avoid duplication during merging operations
    dlrs_details=dealer_list[['Dlrcode','Dealer Name','Region','Channel Name']].drop_duplicates(subset=['Dlrcode'], keep='first')
    dlrs_details.set_index('Dlrcode', inplace=True)
    
    
    
    
    #                |||  FRAN -VE Pivot    |||
    pivot_fran_ve_all_msisdns=fd_prepaid_pen[fd_prepaid_pen['Channel'].isin(['VODAFONE SHOPS','EXCLUSIVE DEALERS']) & (fd_prepaid_pen['HuBOter app flag']=='N')].pivot_table(values=['MSISDN','Ποσοστό επίτευξης FD προηγούμενου μήνα','ΠΟΣΟ ΠΑΡΑΚΡΑΤΗΣΗΣ'],index=['DLR Code'],\
                               aggfunc={'ΠΟΣΟ ΠΑΡΑΚΡΑΤΗΣΗΣ':'sum','MSISDN':'count','Ποσοστό επίτευξης FD προηγούμενου μήνα':'mean'}).sort_values(by='ΠΟΣΟ ΠΑΡΑΚΡΑΤΗΣΗΣ')
    
    pivot_fran_ve_FD_yes=fd_prepaid_pen[(fd_prepaid_pen['Channel'].isin(['VODAFONE SHOPS','EXCLUSIVE DEALERS'])) & (fd_prepaid_pen['Full Digital']=='Y') & (fd_prepaid_pen['HuBOter app flag']=='N') ].pivot_table(values='MSISDN',index='DLR Code', aggfunc='count')
    
    pivot_fran_ve=pd.merge(pivot_fran_ve_all_msisdns,pivot_fran_ve_FD_yes,how='left',on='DLR Code', suffixes=("_Totals","_Full Digital"))
      
    pivot_fran_ve['Ποσοστό Επίτευξης Full Digital (%)']=round(pivot_fran_ve['MSISDN_Full Digital']/pivot_fran_ve['MSISDN_Totals'],2)
        
    pivot_fran_ve=pd.merge(pivot_fran_ve,dlrs_details,how='left',left_index=True, right_index=True) # bringing channel and the rest from dealer list

    
    pivot_fran_ve=pivot_fran_ve[['Dealer Name','Channel Name','Region','MSISDN_Totals','MSISDN_Full Digital','Ποσοστό Επίτευξης Full Digital (%)',
                       'Ποσοστό επίτευξης FD προηγούμενου μήνα','ΠΟΣΟ ΠΑΡΑΚΡΑΤΗΣΗΣ']] # rearragning columns to match the template
    
    
    #             |||      OWN Pivot     |||
    
    pivot_own_all_MSISDNS=fd_prepaid_pen[(fd_prepaid_pen['Channel']=='OWN SHOPS')& (fd_prepaid_pen['HuBOter app flag']=='N')].pivot_table(values=['MSISDN'],index=['POS code'], aggfunc='count')
    pivot_own_full_Digital_yes=fd_prepaid_pen[(fd_prepaid_pen['Channel']=='OWN SHOPS') & (fd_prepaid_pen['Full Digital']=='Y')& (fd_prepaid_pen['HuBOter app flag']=='N')].pivot_table(values=['MSISDN'],
                                                                                                                                        index=['POS code'], aggfunc='count')
    pivot_own=pd.merge(pivot_own_all_MSISDNS,pivot_own_full_Digital_yes, how='left', on='POS code', suffixes=('_Totals','_Full Digital'))
    pivot_own['Ποσοστό Επίτευξης Full Digital']=round(pivot_own['MSISDN_Full Digital']/pivot_own['MSISDN_Totals'],2)
    pivot_own['Channel']='OWN SHOPS'
    pivot_own=pivot_own[['Channel', 'MSISDN_Totals','MSISDN_Full Digital','Ποσοστό Επίτευξης Full Digital']]
    pivot_own.sort_values(by='Ποσοστό Επίτευξης Full Digital', inplace=True)
    
    
    
    #        |||    Wholesale Pivot      |||
    
    
    wholesale_dict= {'DLR CODE':['DLR9167',
                                 'DLR6111',
                                 'DLR6631',
                                 'DLR8946',
                                 'DLR7012',
                                 'DLR8676',
                                 'DLR8766',
                                 'DLR8027',
                                 'DLR7146',
                                 'DLR9026',
                                 'DLR8141',
                                 'DLR9218']}
    
    wholesale_df=pd.DataFrame(wholesale_dict)
    wholesale_df.set_index('DLR CODE', inplace=True)
    
    wholesale_df=pd.merge(wholesale_df,dlrs_details,left_index=True, right_index=True)
    
    wholesale_pen=fd_prepaid_pen[fd_prepaid_pen['Channel'].isin(['INDIRECT SALES','BUSINESS INDIRECT NEW'])]
    wholesale_pen.reset_index(drop=True,inplace=True)
    
    wholesale_pen_pivot_all_MSISDNs= wholesale_pen.pivot_table(values=['MSISDN'],index=['DLR Code'], aggfunc='count')
    wholesale_pen_pivot_FD_yes_MSISDNs= wholesale_pen[wholesale_pen['Full Digital']=='Y'].pivot_table(values=['MSISDN'],index=['DLR Code'], aggfunc='count')
    
    wholesale_df=wholesale_df.merge(wholesale_pen_pivot_all_MSISDNs,how='left',left_index=True,right_index=True)\
                             .merge(wholesale_pen_pivot_FD_yes_MSISDNs,how='left',left_index=True,right_index=True, suffixes=('_Totals','_Full Digital'))
    
    wholesale_df.reset_index(inplace=True)
    wholesale_df.rename(columns={'index': 'DLR CODE'},inplace=True)

    wholesale_df['Full Digital Percentage']=round(wholesale_df['MSISDN_Full Digital']/wholesale_df['MSISDN_Totals'],2)
    
        
    wholesale_df['Ποσοστό επίτευξης Full Digital Προηγούμενου μήνα']=pd.merge(wholesale_df,last_month_fd_score_ws,how='left',on='DLR CODE')['Full Digital Percentage ']
    
    wholesale_df.fillna(value=0, inplace=True)
    
    def wholesale_pen_calc(df,i):
        
        if  df.loc[i,'MSISDN_Totals']-df.loc[i,'MSISDN_Full Digital']>=100:
            if df.loc[i,'Full Digital Percentage'] <0.9:
                if df.loc[i,'Ποσοστό επίτευξης Full Digital Προηγούμενου μήνα'] < 0.9:
                    return -(df.loc[i,'MSISDN_Totals']-df.loc[i,'MSISDN_Full Digital'])*8
                else:
                    return -(df.loc[i,'MSISDN_Totals']-df.loc[i,'MSISDN_Full Digital'])*4
            else:
                return 0
        else:
            if df.loc[i,'Full Digital Percentage'] <0.9:
                if df.loc[i,'Ποσοστό επίτευξης Full Digital Προηγούμενου μήνα'] < 0.9:
                    return -(df.loc[i,'MSISDN_Totals']-df.loc[i,'MSISDN_Full Digital'])*5
                else:
                    return -(df.loc[i,'MSISDN_Totals']-df.loc[i,'MSISDN_Full Digital'])*2.5
            else:
                return 0
    
    
    for i in range(wholesale_df.shape[0]): 
        
        wholesale_df.loc[i,'Penalty']= wholesale_pen_calc(wholesale_df,i)
    
    wholesale_df['diff']=wholesale_df['MSISDN_Totals']-wholesale_df['MSISDN_Full Digital']
    
    
    dlr_tobe_penalized=wholesale_df[wholesale_df['Penalty']<0]['DLR CODE'].tolist()
    diff_msisdn = wholesale_df[wholesale_df['Penalty']<0]['diff'].tolist()
    penalty_ws= wholesale_df[wholesale_df['Penalty']<0]['Penalty'].tolist()
    
    for index,item in enumerate(dlr_tobe_penalized):
                
        targeted_rows=(wholesale_pen['DLR Code']==item) & (wholesale_pen['Full Digital']=='N')
        
        wholesale_pen.loc[targeted_rows,'ΠΟΣΟ ΠΑΡΑΚΡΑΤΗΣΗΣ']=penalty_ws[index]/diff_msisdn[index]
    
    
    
    
    # Exporting

    #### calling the ExcelWriter method so that I can write multiple sheets on the same xlsx file
    
    m=input('Please specify month (e.g Jan21): ')
    
    
    writer=pd.ExcelWriter(input_dir + '\\' + 'FD Penalty ' + str(m) + ' per MSISDN_pyp.xlsx', engine='xlsxwriter')
    
    fd_prepaid_pen.to_excel(writer,index=False, sheet_name='All data (VF-VE) penalty')
    wholesale_pen.to_excel(writer,index=False, sheet_name='All data (Wholesale) penalty')
    pivot_fran_ve.to_excel(writer,index= True, sheet_name='Fran-VE')
    pivot_own.to_excel(writer, index= True, sheet_name= 'Own summary')
    wholesale_df.iloc[:,0:9].to_excel(writer, index=False, sheet_name= 'Wholesale' )
    writer.save()
    
    print("\nFile exported at: \n", input_dir)

    t1=time.time()
    
    print('\nTime Elapsed: ', t1-t0, 'sec')
    
    return list([fd_prepaid_pen,pivot_fran_ve,pivot_own,wholesale_df,wholesale_pen])


result=FD_penalty_calc(fd_prepaid_report, dealer_list, ha_bundles, youth_bo, vsa_bo, last_month_fd_score)


# Results summary 
print(result[0].groupby('Channel')['ΠΟΣΟ ΠΑΡΑΚΡΑΤΗΣΗΣ'].sum())
print(result[3])

result[1].iloc[0:15,:].plot(kind='barh',x='Dealer Name',y='ΠΟΣΟ ΠΑΡΑΚΡΑΤΗΣΗΣ')





