'''
HiveDB에 있는 Table을 Owner와 Created_time을 함께 조회하여
old table을 정리하고자 하는 script
'''


import pandas as pd

from datetime import datetime
from hyspark import Hyspark
from time import strptime
from utils.spark_hive_utils import *


def get_employee_id():
  from os import getcwd
  from re import search
  return str(search(r'.+(\d{6}).?', getcwd()).group(1))


now= datetime.now()
curr_time = now.strftime('%H:%M:%S')

employee_id = get_employee_id()
hs = HySpark(f'{employee_id}_seqdata_tokenizer_LLM_{curr_time}',
             mem_per_core=8, instance='general')

hc, sc, ss = hs.hive_context, hs.spark_context, hs.spark_session
check_hive_available(hc)

db_name = 'mydb'
query = hc.sql(f'SHOW TABLES FROM {db_name}')
TableNames = df_as_pandas_with_pyspark(query, double_to_float=True, bigint_to_int=True)

new_columns = {'Owner':[], 'Created Time':[]}
for table_name in TableNames.tableName:
    tmp_df = hc.sql(f'DESCRIBE EXTENDED {db_name}.{table_name}')

    Owner, CreateTime = tmp_df.filter(tmp_df.col_name.isin(['Owner', 'Created Time'])).collect()

    # Owner
    new_columns['Owner'].append(list(Owner.asDict().items())[1][1])

    # Created_time
    CreateTime = list(CreateTime.asDict().items())[1][1]
    CreateTime = CreateTime.split()
    month_number = str(strptime(CreateTime[1], '%b').tm_mon)
    if len(month_number) < 2:
        month_number = '0' + month_number

    new_columns['Created Time'].append(f'{CreateTime[-1]}-{month_number}-{CreateTime[2]} {CreateTime[3]}')

TableNames = TableNames.drop(columns=['database', 'isTemporary'])
TableNames = pd.concat([TableNames, pd.DataFrame(new_columns)], axis=1)

TableNames = TableNames[TableNames['Owner']=='myid'].sort_values(by='Created Time', ascending=False)

