# -*- coding: utf-8 -*-
"""
Created on Wed May 25 08:30:53 2022

@author: SamoladasN

"""


import pandas as pd
import datetime
import sqlite3


def reading_files (first_recharges_path , dealer_list_path,youth_report_path,paso_additions_path, trg_path) :
    
    print('\nReading first_recharges file...')
    first_recharges = pd.read_csv(first_recharges_path,sep=',')
    print(first_recharges)
    
    first_recharges.rename(columns={'POS code':'POS code (on Connection)',
                                    'Dealer':'Dealer (on Connection)'},
                                       inplace=True)
    
    
    
    print('\nReading youth report..')
    youth_report = pd.read_csv(youth_report_path,sep=';')
    youth_report.reset_index(drop=True,inplace=True)
    print(youth_report)
    
    print('\nReading paso additions...')
    paso_additions = pd.read_excel(paso_additions_path)
    print(paso_additions)
    
    
    print('\nReading Dealer List...')
    dealer_list = pd.read_excel(dealer_list_path,sheet_name='dl')
    
    
    print('\nReading Indirect List...')
    trg= pd.read_excel(trg_path,sheet_name='ind')
    
    print('\nFile Read Complete!')

    print('\nEstablishing Connection with database...\n')
    conn = sqlite3.connect(r'C:\Users\samoladasn\OneDrive - Vodafone Group\The Prepay team (N&D)\Training_Manuals_Forms\Scripts\SQLite\Prepaid DB.db')
    
    return first_recharges, dealer_list ,trg ,youth_report,paso_additions,conn


def processing_input_files (reading_files): 
    
    first_rechargers, dealer_list, trg,youth_report,paso_additions, conn = reading_files
    
    
    #first_rechargers 
    # first_rechargers.drop(columns=['Subchannel (on recharge)','Channel (on Connection)'],inplace=True,axis=1)
    first_rechargers=first_rechargers[first_rechargers['XPostpay flag']=='Prepay']
    first_rechargers=first_rechargers[first_rechargers['POS code (on Connection)']!='*']
    first_rechargers.drop_duplicates(subset=['MSISDN'],inplace=True,ignore_index=True)
    first_rechargers['MSISDN']=first_rechargers['MSISDN'].astype(str) # since MSISDN is the key between all the files I enforced the data type to be STR in all files to avoid type clashes and problems during merge
    
    #youth processing 
    youth_report['MSISDN']=youth_report['MSISDN'].astype(str)
    paso_additions['MSISDN']=paso_additions['MSISDN'].astype(str)
    youth=pd.concat([paso_additions,youth_report],ignore_index=True)
    youth.drop_duplicates(subset=['MSISDN'],inplace=True,ignore_index=True)
    youth = youth[['MSISDN','Completion Date']]
        
    # dealer list
    dealer_list=dealer_list[['Jup Code','Poscode','Dlrcode','Dealer Name','Region','Channel Name']]
    dealer_list.drop_duplicates(['Poscode'],keep='first',inplace=True,ignore_index=True)
    
    #trg indirect pos
    
    indirect_pos = list(set(trg['POS'])) #keeping only the unique values from the file of trg
    
    #bring new columns
    first_rechargers = pd.merge(first_rechargers,youth[['MSISDN']],on='MSISDN',how='left',indicator='Student Flag')
    first_rechargers['Student Flag']=first_rechargers['Student Flag'].map({'both':True,'left_only':False,'right_only':False})
    
    first_rechargers = pd.merge(first_rechargers,youth[['MSISDN','Completion Date']],on='MSISDN',how='left')
    
    first_rechargers = pd.merge(first_rechargers,dealer_list[['Poscode','Dlrcode','Dealer Name','Region','Channel Name']],
                                how='left',left_on='POS code (on Connection)', right_on='Poscode')
    
    first_rechargers.replace(to_replace=['ΑΤΤΙΚΗ 1','ΑΤΤΙΚΗ 2'],value='ΑΤΤΙΚΗ',inplace=True)
    
    
    first_rechargers.loc[first_rechargers['POS code (on Connection)'].isin(indirect_pos),'Channel Name'] = 'INDIRECT'
    
    base_one_plus_one = pd.read_sql(""" 
                                    
                                    SELECT DISTINCT (MSISDN), Month AS Batch 
                                    FROM projects
                                    WHERE Project="1+1"
                                    
                                    """, conn)
    base_one_plus_one.rename(columns={'Batch':'1+1 Batch'},inplace=True)
    first_rechargers = pd.merge(first_rechargers,base_one_plus_one[['MSISDN','1+1 Batch']],on='MSISDN',how='left')
    
    return first_rechargers
    

def data_processing(processing_input_files):
    
    first_rechargers = processing_input_files
    
    
    digital_sales_pos = ['POS15009','POS10150','POS18959','POS17437','POS18136','POS20331','POS20368']
    
    
    # digital_reg_wholesale_pos = ['POS20560','POS20561','POS20566','POS20567','POS20568','POS20569','POS20572','POS20576','POS20574','POS20578',
                                 # 'POS17224','POS20417','POS20570']
    
    
    wholesale_codes = ['WB17','WB21','WB24','WB27','WAD01','WR01','WR07','WR08','WR09','WR10','WR100','WR102','WR105','WR11','WR12','WR14','WR15',
                       'WR16','WR17','WR18','WR20','WR200','WR21','WR23','WR24','WR25','WR27','WR28','WR29','WR30','WR33','WR35','WR36','WR38','WR39',
                       'WR43','WR44','WR45','WR46','WR47','WR49','WR50','WR53','WR54','WR55','WR56','WR57','WR60','WR61','WR62','WR63','WR65','WR66',
                       'WR68','WR71','WR72','WR76','WR77','WR78','WR81','WR82','WR84','WR87','WR89','WR90','WR92','WR94','WR96','WR97','WR98','WH05',
                       'WH12','WH19','WH20','WH21','WE01','WAU01','WH13','WAR01','WAA01','WB02','WN01','WT01','WC01','WH24','WX01','WA21','WG02',
                       'WAW01','WAU02','WAU03','WAY01','WAT01']
    
    
    
    first_rechargers.loc[(first_rechargers['Dealer (on Connection)'].isin(wholesale_codes) | first_rechargers['Dealer (on Connection)'].str.startswith('WR')),'Channel Name']='WHOLESALE'
    first_rechargers.loc[(first_rechargers['Dealer (on Connection)'].isin(wholesale_codes) | first_rechargers['Dealer (on Connection)'].str.startswith('WR')),'Subchannel']='Wholesale'
    
    first_rechargers.loc[first_rechargers['Dealer (on Connection)'].str.startswith('LMN'),'Channel Name']='TAZA'
    first_rechargers.loc[first_rechargers['Dealer (on Connection)'].str.startswith('LMN'),'Subchannel']='TAZA'

    
    conditions_sala =   (first_rechargers['Dealer (on Connection)'].str.contains('HUN')==False)  & \
                        (first_rechargers['Dealer (on Connection)'].str.contains('STR')==False)  & \
                        (first_rechargers['Dealer (on Connection)'].str.contains('FREE')==False) & \
                        (first_rechargers['Dealer (on Connection)'].str.contains('WAR')==False)  & \
                        (first_rechargers['Dealer (on Connection)'].str.contains('PROM')==False) & \
                        (first_rechargers['Channel Name'].isin(['VODAFONE SHOPS','EXCLUSIVE DEALERS','OWN SHOPS','INDIRECT']))
                    
    conditions_hunter = first_rechargers['Dealer (on Connection)'].str.contains('HUN')
    conditions_streetcat =  first_rechargers['Dealer (on Connection)'].str.contains('STR')
    conditions_student = first_rechargers['Student Flag']==True
    conditions_warriors = (first_rechargers['Dealer (on Connection)'].str.contains('WAR')) & (first_rechargers['Channel Name'].isin(['WHOLESALE'])==False) #DIAMANTAKOS code BEGINS WITH WAR and needs to be excluded
    conditions_digital_reg = first_rechargers['Dealer (on Connection)'].str.endswith('DR')
    conditions_free_sims =  first_rechargers['Dealer (on Connection)'].str.contains('FREE')
    conditions_promoters =  first_rechargers['Dealer (on Connection)'].str.contains('PROM')
    
    
    
    first_rechargers.loc[conditions_sala,'Subchannel'] = 'Sala' 
    first_rechargers.loc[conditions_hunter,'Subchannel'] = 'Hunter'
    first_rechargers.loc[conditions_streetcat ,'Subchannel'] = 'Streetcat'
    first_rechargers.loc[conditions_warriors,'Subchannel']= 'Warriors'
    first_rechargers.loc[conditions_digital_reg,'Subchannel']= 'Digital Registration'
    first_rechargers.loc[conditions_free_sims,'Subchannel']='Free Sim'
    first_rechargers.loc[conditions_promoters,'Subchannel']='Promoters'
    first_rechargers.loc[first_rechargers['POS code (on Connection)'].isin(digital_sales_pos),'Subchannel']='Digital Sales'
    
    first_rechargers.loc[first_rechargers['Subchannel'].isin(['Digital Registration','Digital Sales']),'Channel Name']='DIGITAL'

                         
    first_rechargers['Subchannel'].fillna('REST',inplace=True)
    
    return first_rechargers
    

def aggregation_and_frc(data_processing,wrk_days_elapsed, wrk_days_remaining):
    
    first_rechargers = data_processing
    
    def linear_frc(actual):
        
        frc = actual + (actual / wrk_days_elapsed) * wrk_days_remaining
        
        return frc
    
    

    # Pivot #1
    pivot_channel = first_rechargers.pivot_table(values='MSISDN',index='Channel Name',aggfunc='count')
    
    # Pivot #2
    pivot_subchannel = first_rechargers[first_rechargers['Subchannel'].isin(['Not Available'])==False].pivot_table(values='MSISDN',index=['Channel Name','Subchannel'],aggfunc='count') 
    
    #creating conditions for sala & region
    region_channel_sala =    first_rechargers[(first_rechargers['Region'].isin(['ΑΤΤΙΚΗ','ΝΟΤΙΟΔΥΤΙΚΗ ΕΛΛΑΔΑ','ΚΡΗΤΗ & ΝΗΣΙΑ ΑΙΓΑΙΟΥ','ΒΟΡΕΙΑ ΕΛΛΑΔΑ']))&\
                            (first_rechargers['Channel Name'].isin(['VODAFONE SHOPS','EXCLUSIVE DEALERS','OWN SHOPS','INDIRECT']))&\
                            (first_rechargers['Subchannel']=='Sala') &\
                            (first_rechargers['Student Flag']==False)]
    
    region_channel_sala.reset_index(drop=True,inplace=True)

    #Pivot #3
    pivot_region_channel_sala = region_channel_sala.pivot_table(values='MSISDN',index=['Region','Channel Name'],aggfunc='count')
    
    
    #pivot #4
    
    pivot_youth=first_rechargers.pivot_table(values='MSISDN',index='Student Flag',aggfunc='count')
    
    # applying linear frc to the pivot tables
    
    pivot_list =   [pivot_channel,
                    pivot_subchannel,
                    pivot_region_channel_sala,
                    pivot_youth]
    
    
    for pivots in pivot_list: 
        print(pivots)
        pivots.rename(columns={'MSISDN':'Actual'},inplace=True)
        pivots['Linear FRC']=linear_frc(pivots['Actual'])
    
    
    return first_rechargers,pivot_list



def file_export(output_dir): 
    writer=pd.ExcelWriter(output_dir + '\\' + 'Daily Prepay 1st recharges {}.xlsx'.format(datetime.date.today().strftime("%d-%m-%Y")), engine='xlsxwriter')
    
    df[0].to_excel(writer,index=False, sheet_name='All data')

    df[1][0].to_excel(writer,index=True, sheet_name='per Channel')
    df[1][1].to_excel(writer,index= True, sheet_name='per Subchannel')
    df[1][2].to_excel(writer, index= True, sheet_name= 'Sala per Region')   
    df[1][3].to_excel(writer, index= True, sheet_name= 'Student') 
    writer.save()




df = aggregation_and_frc(wrk_days_elapsed = 7,wrk_days_remaining =11 ,data_processing=
        data_processing(
            processing_input_files (    
                reading_files(first_recharges_path = r'\MINER_MKT_Share$\Channel Planning Team\Inputs\1st Rechargers files\Daily Prepaid 1st recharge report.csv',
                              youth_report_path = r'C:\Users\samoladasn\Desktop\temp\Paso Report - Youth Sales.csv',
                              paso_additions_path = r'\MINER_MKT_Share$\BI Reports\Youth Sales Report\Input Files\Paso Additions from Siebel.xlsx',
                              dealer_list_path = r'C:\Users\samoladasn\OneDrive\The Prepay team (N&D)\Other Files\Dealer List\Siebel Dealer List 032023.xlsx',
                              trg_path=r'C:\Users\samoladasn\OneDrive\he Prepay team (N&D)\Other Files\Targets\2023\Opening Targets_Apr23.xlsx'))))



file_export(r'C:\Users\samoladasn\OneDrive\The Prepay team (N&D)\Other Files\Brains Files\1st rechargers')

