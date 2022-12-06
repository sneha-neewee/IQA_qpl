###############inserting to main hist table when we don't consider add as a parameter

import datetime
import numpy as np
from db_connection import read_from_db,insert_df_to_db
from db_config import config
import pandas as pd
#import xlrd
df_db = read_from_db(''' select * from public.iqa_qpl_snehatemp_hist_table ''',config)
df_xls = pd.read_excel(r'C:\Users\Sneha\Documents\portescap\referenc_data\\' + 'UPDATED NEW QPL FOR BODHEE SYSTEM (01-Dec-2022).xlsx' ,engine='openpyxl',sheet_name='Sheet1')
df_xls.rename(columns={'Sr.': 'sr_no', 'Drawing rev. as per which QPL is prepared':'drawing', 'Part No':'part_no', 'Part name':'part_name' ,'Commodity': 'commodity' , 'Sample table': 'sample_table', 'Station Category': 'station', 'Parameter': 'parameter', 'Lower limit': 'lower_limit', 'Upper limit': 'upper_limit', 'Instruction': 'instructions', 'Units': 'unit', 'Primary Tool category': 'primary_tool_category', 'Secondary Tool category': 'secondary_tool_category', 'TS size': 'ts_size', 'Validation': 'validation', 'Customer complaint': 'customer_complaint', 'Line complaint': 'line_complaint', 'Regular Rejection caught at Incoming Stage': 'regular_rejection', 'Old complaint': 'old_complaint','Delete':'status'}, inplace=True)
df_xls_add = df_xls[df_xls.status == 'add']

print(df_xls_add)
for i, row in df_xls.iterrows():
    print(i)
    if int(row.sr_no) in df_db.sr_no.astype(int):
        #print(f"S.No {int(row.sr_no)} exists in DB")
        # db_row = df_db[df_db.sr_no.astype(int) == int(row.sr_no)]
        df_db.loc[df_db['sr_no'] == row.sr_no, ['effective_end_date','status']] = datetime.datetime.now(), False
        row['effective_start_date'] = datetime.datetime.now()
        row['effective_end_date'] = pd.Timestamp('')
        row['status'] = True
        #df_db = df_db[~(df_db.sr_no.astype(int) == int(row.sr_no))]
        df_db = df_db.append(row)
    else:
        print("test")
        row['effective_start_date'] = datetime.datetime.now()
        row['status'] = True
        df_db = df_db.append(row)
insert_df_to_db(df_db,'iqa_qpl_snehatemp_hist_table',10000,config, 'public', 'replace')



