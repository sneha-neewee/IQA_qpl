import datetime
import numpy as np
from db_connection import read_from_db
from db_config import config
import pandas as pd
import xlrd
df_xls = pd.read_excel(r'C:\Users\Sneha\Documents\portescap\referenc_data\\' + 'UPDATED NEW QPL FOR BODHEE SYSTEM.xlsx',engine='openpyxl',sheet_name='Sheet1')
df_xls.to_csv(r'C:\Users\Sneha\Documents\portescap\referenc_data\30_Nov_qplmaster.csv',index=False)
df_xls = pd.read_csv(r'C:\Users\Sneha\Documents\portescap\referenc_data\30_Nov_qplmaster.csv')
df_xls.rename(columns={  'Sr.' : 'sr_no', 'Drawing rev. as per which QPL is prepared':'drawing', 'Part No':'part_no', 'Part name':'part_name', 'Commodity': 'commodity', 'Sample table': 'sample_table', 'Station Category': 'station', 'Parameter': 'parameter', 'Lower limit': 'lower_limit', 'Upper limit': 'upper_limit', 'Instruction': 'instructions', 'Units': 'unit', 'Primary Tool category': 'primary_tool_category', 'Secondary Tool category': 'secondary_tool_category', 'TS size': 'ts_size', 'Validation': 'validation', 'Customer complaint': 'customer_complaint', 'Line complaint': 'line_complaint', 'Regular Rejection caught at Incoming Stage': 'regular_rejection', 'Old complaint': 'old_complaint',    }, inplace=True)
df_xls.dropna(axis=0, how='all',thresh=None,subset=None,inplace=True)
df_db = read_from_db(''' select * from public.iqa_qpl_snehatemp_hist_table ''',config)
df_db_true = df_db[df_db.status == True]
df_db_false = df_db[df_db.status == False]
df_xls[['effective_start_date','effective_end_date','status']] = datetime.datetime.now(),pd.Timestamp(''),True
df_xls['sr_no'].astype('int')
f = df_db_true.append(df_xls)
is_dup= f.duplicated(subset=['sr_no','part_no','parameter'],keep='last')
is_row_dup= is_dup.duplicated(subset=['sr_no', 'drawing', 'part_no', 'part_name', 'commodity', 'sample_table',
       'station', 'parameter', 'lower_limit', 'upper_limit', 'instructions',
       'unit', 'primary_tool_category', 'secondary_tool_category', 'ts_size',
       'validation', 'customer_complaint', 'line_complaint',
       'regular_rejection'],keep='last')
f.loc[~is_row_dup,'effective_end_date']= pd.Timestamp('')
f.loc[~is_row_dup,'status']= False
is_row_dup[status == False].drop("Retake",axis=0,inplace=True)
final_df = df_db_false.append(f)
#final_df.dropna(axis=0, how='all',thresh=None,subset=None,inplace=True)
final_df.to_csv(r"C:\Users\Sneha\Documents\portescap\referenc_data\final_df.csv")