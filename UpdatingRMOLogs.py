# -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 15:02:44 2024

@author: GADG3559
"""

import pandas as pd
import os
import warnings
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import datetime
import gspread

warnings.filterwarnings("ignore")
pd.set_option("display.max_column", 100)
pd.set_option("display.max_row", 100)

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import pickle

BQUERY_CREDS = '/service-account-key/application_default_credentials_anup.json'

def read_file(fromdate, todate):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly']
    
    
    gc = gspread.service_account(filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    spreadsheetID = '1F6Adv9BKwrLAQ_naJXDdIYUktVLdcagJx-dAc2n-nZY'
    
    gs = gc.open_by_key(spreadsheetID)
    
    import pandas_gbq
    client = bigquery.Client(
        project="cdot-sandbox-ctio-anup", 
        credentials=load_credentials_from_file(BQUERY_CREDS)[0]
        )

    sql = '''
    DECLARE fromdate DATETIME DEFAULT '{}';
    DECLARE todate DATETIME DEFAULT '{}';

    SELECT section_rate_uuid, section_name,
            DATETIME(effective_time, 'America/Denver') as effective_time,
            DATETIME(expired_time, 'America/Denver') as expired_time,
            final_fare.fare.text_value,
            s1.segment_rate_uuid, final_fare,
            seg.segment_name,
            seg.created_timestamp,
            seg.schedule_id,
            seg.schedule_name,
            seg.schedule_type,
            seg.rate_id,
            seg.rate_name,
            seg.rate_type,
            seg.previous_toll,
            seg.calculated_toll,
            seg.final_toll,
            payment_type_agency,
            seg.traffic_data.start_time,
            seg.traffic_data.end_time,
            seg.los_data.s_ml,
            seg.los_data.v_ml,
            seg.los_data.s_gp,
            seg.los_data.v_gp,
            seg.los_data.t_ml,
            seg.los_data.t_gp,
            seg.los_data.los_ml,
            seg.los_data.los_gp,
            seg.los_data.los,
            seg.multiplier,
            seg.calculation_detail    
    FROM `e470-hpte.hpte_prod.rp_section_rate` as sec
    LEFT JOIN sec.segment_rate as s1
    LEFT JOIN (SELECT * FROM `e470-hpte.hpte_prod.rc_segment_rate`
                    WHERE created_timestamp BETWEEN TIMESTAMP(fromdate, 'America/Denver') AND TIMESTAMP(todate, 'America/Denver')) as seg
    on s1.segment_rate_uuid = seg.segment_rate_uuid
    WHERE sec.effective_time BETWEEN TIMESTAMP(fromdate, 'America/Denver') AND TIMESTAMP(todate, 'America/Denver')
    AND expired_time IS NOT NULL
    --AND section_name LIKE "I70C%"
    --AND segment_name = "I70C_Z1WB"
    --AND payment_type_agency = "AVI"
    --AND final_fare.fare.text_value LIKE "%FAIL%"
    AND final_fare.section_rate_type = "RETRO_OVERRIDE"
    -- AND section_rate_uuid = "b36ae0ee-e9a3-4f2d-ab4f-f8e44b371e43"
    ORDER BY effective_time
    '''.format(fromdate.strftime('%Y-%m-%d %H:%M:%S'), todate.strftime('%Y-%m-%d %H:%M:%S'))

    query_job = client.query(sql)#.to_dataframe()  # Make an API request.

    df = query_job.to_dataframe()
   

    return gs,df



def updating_RMO():

    fromdate = datetime.datetime.combine(datetime.datetime.now().date() - datetime.timedelta(days = 1),datetime.time(0,0,0))
    todate = datetime.datetime.combine(datetime.datetime.now().date() - datetime.timedelta(days = 1),datetime.time(23,59,59))
    
    gs,DF = read_file(fromdate, todate)
    
    DF['Facility'] = DF['section_name'].apply(lambda x: x.split('-')[0])
    
    month_number = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
    num_month = {}
    for key, value in month_number.items():
        num_month[value] = key
    
    ### I70C
    
    Sheet = gs.worksheet('I70 C')
    index = len(Sheet.col_values(4))
    
    df = DF[DF['Facility'] == 'I70C']
    
    if df.shape[0] > 0:
        df = df[['section_name','effective_time','expired_time']].drop_duplicates().reset_index()
        df['nextDay'] = df['expired_time'].dt.date != df['effective_time'].dt.date
        df_add = df[df['nextDay']]
        df.loc[df['nextDay'], 'expired_time'] = df[df['nextDay']]['expired_time'].apply(lambda x: datetime.datetime.combine(x.date() - datetime.timedelta(days = 1),datetime.time(23,59,59)))
        df_add['effective_time'] = df_add['effective_time'].apply(lambda x: datetime.datetime.combine(x.date() + datetime.timedelta(days = 1),datetime.time(0,0,0)))
        df = pd.concat([df,df_add])
        df = df.sort_values(by = 'effective_time').reset_index()
        df['Year'] = df['effective_time'].dt.year
        df['Facility'] = 'I70C'
        df['Dir'] = df['section_name'].apply(lambda x: 'EB' if 'EB' in x else 'WB')
        df['time'] = (((df['expired_time'] - df['effective_time']).dt.total_seconds())/60).astype('int64')
        df['Date'] = df['effective_time'].apply(lambda x: str(x.day)+'-'+num_month[x.month])
        df['effective_time'] = df['effective_time'].apply(lambda x: str(x.hour) +':'+"{0:0=2d}".format(x.minute))
        df['expired_time'] = df['expired_time'].apply(lambda x: str(x.hour) +':'+"{0:0=2d}".format(x.minute))
        df = df[['Year','Facility','Dir','Date','effective_time','expired_time','time']]
        Sheet.update(Sheet.cell(index+1, 1).address, df.values.tolist())
        
    ###I25GAP
    Sheet = gs.worksheet('I25GAP')
    index = len(Sheet.col_values(4))
    
    df = DF[DF['Facility'] == 'I25GAP']
    
    if df.shape[0]>0:
        df = df[['section_name','effective_time','expired_time']].drop_duplicates().reset_index()
        df['nextDay'] = df['expired_time'].dt.date != df['effective_time'].dt.date
        df_add = df[df['nextDay']]
        df.loc[df['nextDay'], 'expired_time'] = df[df['nextDay']]['expired_time'].apply(lambda x: datetime.datetime.combine(x.date() - datetime.timedelta(days = 1),datetime.time(23,59,59)))
        df_add['effective_time'] = df_add['effective_time'].apply(lambda x: datetime.datetime.combine(x.date() + datetime.timedelta(days = 1),datetime.time(0,0,0)))
        df = pd.concat([df,df_add])
        df = df.sort_values(by = 'effective_time').reset_index()
        df['Year'] = df['effective_time'].dt.year
        df['Facility'] = '25GP'
        df['Dir'] = df['section_name'].apply(lambda x: 'NB' if 'NB' in x else 'SB')
        df['Zone1'] = df['section_name'].apply(lambda x: x.split('-')[1][0:2])
        df['Zone2'] = df['section_name'].apply(lambda x: x.split('-')[3][0:2])
        df['Zone'] = df.apply(lambda x: x.Zone1 if x.Zone1 == x.Zone2 else 'ALL',axis = 1)
        df['time'] = (((df['expired_time'] - df['effective_time']).dt.total_seconds())/60).astype('int64')
        df['Date'] = df['effective_time'].apply(lambda x: str(x.day)+'-'+num_month[x.month])
        df['Effective_time'] = df['effective_time'].apply(lambda x: str(x.hour) +':'+"{0:0=2d}".format(x.minute))
        df['expired_time'] = df['expired_time'].apply(lambda x: str(x.hour) +':'+"{0:0=2d}".format(x.minute))
        df = df[['Year','Facility','Dir','Zone','Date','effective_time','Effective_time','expired_time','time']]
        df = df.groupby(['Year','Facility','Dir','Date','effective_time','Effective_time','expired_time','time'])['Zone'].apply(list).reset_index()
        df['Zone'] = df['Zone'].apply(lambda x: x[0] if len(x) == 1 else 'ALL')
        df = df.sort_values(by = 'effective_time').reset_index()
        df = df[['Year','Facility','Dir','Zone','Date','Effective_time','expired_time','time']]
        Sheet.update(Sheet.cell(index+1, 1).address, df.values.tolist())
        
    ###MEXL WB

    Sheet = gs.worksheet('MEXL WB')
    index = len(Sheet.col_values(4))
    
    df = DF[DF['Facility'] == 'WBMEXL']
    
    if df.shape[0] > 0:
        df = df[['section_name','effective_time','expired_time']].drop_duplicates().reset_index()
        df['nextDay'] = df['expired_time'].dt.date != df['effective_time'].dt.date
        df_add = df[df['nextDay']]
        df.loc[df['nextDay'], 'expired_time'] = df[df['nextDay']]['expired_time'].apply(lambda x: datetime.datetime.combine(x.date() - datetime.timedelta(days = 1),datetime.time(23,59,59)))
        df_add['effective_time'] = df_add['effective_time'].apply(lambda x: datetime.datetime.combine(x.date() + datetime.timedelta(days = 1),datetime.time(0,0,0)))
        df = pd.concat([df,df_add])
        df = df.sort_values(by = 'effective_time').reset_index()
        df['Year'] = df['effective_time'].dt.year
        df['Facility'] = '70W'
        df['time'] = (((df['expired_time'] - df['effective_time']).dt.total_seconds())/60).astype('int64')
        df['Date'] = df['effective_time'].apply(lambda x: str(x.day)+'-'+num_month[x.month])
        df['effective_time'] = df['effective_time'].apply(lambda x: str(x.hour) +':'+"{0:0=2d}".format(x.minute))
        df['expired_time'] = df['expired_time'].apply(lambda x: str(x.hour) +':'+"{0:0=2d}".format(x.minute))
        df = df[['Year','Facility','Date','effective_time','expired_time','time']]
        Sheet.update(Sheet.cell(index+1, 1).address, df.values.tolist())
        






    
