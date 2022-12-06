from db_connection import read_from_db,insert_df_to_db
from db_config import config
import datetime
import pandas as pd
import numpy as np
import xlrd
df_db = read_from_db(''' select * from public.iqa_qpl_snehatemp_hist_table ''',config)
df_xls = pd.read_excel(r'C:\Users\Sneha\Documents\portescap\referenc_data\\' + 'UPDATED NEW QPL FOR BODHEE SYSTEM (01-Dec-2022).xlsx' ,engine='openpyxl',sheet_name='Sheet1')
df_xls.rename(columns={'Sr.': 'sr_no', 'Drawing rev. as per which QPL is prepared':'drawing', 'Part No':'part_no', 'Part name':'part_name' ,'Commodity': 'commodity' , 'Sample table': 'sample_table', 'Station Category': 'station', 'Parameter': 'parameter', 'Lower limit': 'lower_limit', 'Upper limit': 'upper_limit', 'Instruction': 'instructions', 'Units': 'unit', 'Primary Tool category': 'primary_tool_category', 'Secondary Tool category': 'secondary_tool_category', 'TS size': 'ts_size', 'Validation': 'validation', 'Customer complaint': 'customer_complaint', 'Line complaint': 'line_complaint', 'Regular Rejection caught at Incoming Stage': 'regular_rejection', 'Old complaint': 'old_complaint',}, inplace=True)
df_xls = df_xls.drop(['Delete'], axis=1)
df_xls.dropna(axis=0, how='all',thresh=None,subset=None,inplace=True)
df_xls[['effective_start_date','effective_end_date','status']] = datetime.datetime.now(),pd.Timestamp(''),True
f = df_db.append(df_xls)
# is_dup_last= f.duplicated(subset=['sr_no','status'],keep='last')
# f.loc[is_dup,'effective_end_date']= datetime.datetime.now()
# f.loc[is_dup,'status']= False

true_stat = f.loc[f['status'] == True]
dup_1st = true_stat.duplicated(subset=['sr_no'],keep='last')
dup_last = true_stat.duplicated(subset=['sr_no'],keep='first')

f.loc[dup_1st,'effective_end_date']= datetime.datetime.now()
f.loc[dup_1st,'status']= False
f.loc[dup_last,'effective_start_date']= datetime.datetime.now()
f.loc[dup_last,'effective_end_date']= pd.Timestamp('')
f.loc[dup_last,'status']= True
#df_hist = df_db.replace(f, inplace= True)
f.to_csv(r"C:\Users\Sneha\Documents\portescap\referenc_data\final_df.csv" , index=False)
insert_df_to_db(f,'iqa_qpl_snehatemp_hist_table',10000,config, 'public', 'replace')
