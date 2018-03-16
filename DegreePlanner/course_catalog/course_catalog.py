import numpy as np
import pandas as pd
import os
from datetime import date, datetime
from sqlalchemy import create_engine

# local connection information
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
engine = create_engine(f'mssql+pyodbc://{db_user}:{db_pass}' +
                       '@PSC-SQLProd/Campus6?' +
                       'driver=ODBC+Driver+13+for+SQL+Server')
connection = engine.connect()

sections_begin_year = '2011'

sql_str = "SELECT * FROM EVENT WHERE " + \
          "EVENT_STATUS = 'A' " + \
          "AND EVENT_TYPE = 'CLAS' "
df_event = pd.read_sql_query(sql_str, connection)

dfe = df_event[['EVENT_ID', 'EVENT_LONG_NAME', 'DESCRIPTION', ]]

dfe = dfe.rename(columns={'EVENT_LONG_NAME': 'course_name',
                          'EVENT_ID': 'course_id',
                          'DESCRIPTION': 'description'})

sql_str = "SELECT * FROM SECTIONS WHERE " + \
          "EVENT_SUB_TYPE NOT IN ('ADV', 'SI', 'LAB') " + \
          f"AND ACADEMIC_YEAR >= '{sections_begin_year}' " + \
          "AND ACADEMIC_TERM NOT IN ('Fa', 'SP') "
df_sections = pd.read_sql_query(sql_str, connection)

dfs = df_sections[['EVENT_ID', 'EVENT_SUB_TYPE', 'SECTION', 'ACADEMIC_YEAR',
                   'ACADEMIC_TERM', 'ACADEMIC_SESSION', 'CIP_CODE', 'CREDITS',
                   'REVISION_DATE', 'REVISION_TIME',
                   ]]

dfs = dfs.sort_values(['EVENT_ID', 'EVENT_SUB_TYPE', 'ACADEMIC_YEAR',
                       'ACADEMIC_TERM', 'SECTION', 'CREDITS'],
                      ascending=[True, True, True, False, True, True])
dfs = dfs.drop_duplicates(['EVENT_ID', 'ACADEMIC_YEAR', 'ACADEMIC_TERM'],
                          keep='first')
dfs = dfs.sort_values(['EVENT_ID', 'EVENT_SUB_TYPE', 'ACADEMIC_YEAR',
                       'ACADEMIC_TERM', 'SECTION', 'CREDITS'],
                      ascending=[True, True, True, False, True, True])
dfs = dfs.drop_duplicates(['EVENT_ID', 'CREDITS'],
                          keep='first')

dfs = dfs.rename(columns={'EVENT_ID': 'course_id',
                          'CREDITS': 'default_credit_hours',
                          'CIP_CODE': 'course_cip_code'})

df = pd.merge(dfe, dfs, on=['course_id'], how='left')

df = df[~(df['course_id'].str.contains('REG', case=False))]
df = df[~(df['course_id'].str.contains('STDY', case=False))]
df = df[~(df['course_id'].str.contains('PRV TRAN', case=False))]

df.loc[:, 'Level'] = '0' + df.loc[:, 'course_id'].str[-3:-2]
df.loc[:, 'Level'] = df.loc[:, 'Level'].str.replace(' ', '')  # for MAT 98
df.loc[:, 'course_id'] = df.loc[:, 'course_id'].str.replace(' ', '')
df.loc[:, 'description'] = (df.loc[:, 'description'].str.replace('\n', ' ')
                                                    .str.replace('\r', ' ')
                                                    .str.replace('\"\"', "\'")
                                                    .str.replace('\"', "\'")
                            )

df.loc[:, 'status'] = 'ACTIVE'

df['ACADEMIC_YEAR'] = (pd.to_numeric(df['ACADEMIC_YEAR'], errors='coerce')
                         .fillna(sections_begin_year).astype(np.int64))
cat_yr = (lambda c: c['ACADEMIC_YEAR'] if (c['ACADEMIC_TERM'] == 'FALL')
          else (c['ACADEMIC_YEAR'] - 1))
df.loc[:, 'catalog_year'] = df.apply(cat_yr, axis=1)
df.loc[:, 'integration_id'] = (df.loc[:, 'course_id'] + '.' +
                               df.loc[:, 'catalog_year'].apply(str))

df.loc[:, 'default_credit_hours'] = df.loc[:, 'default_credit_hours'].fillna(3)

df = (df.sort_values(['integration_id', 'default_credit_hours'],
                     ascending=[True, False])
      .drop_duplicates(['integration_id'], keep='first')
      )

# set earlier catalog year courses to 'INACTIVE'
max_cat_yr = df.groupby(['course_id'])['catalog_year'].max().reset_index()
max_cat_yr = max_cat_yr.rename(columns={'catalog_year': 'max_cat_yr'})
df = pd.merge(df, pd.DataFrame(max_cat_yr), on=['course_id'], how='left')
df.loc[(df['catalog_year'] != df['max_cat_yr']), 'status'] = 'INACTIVE'

df = df.loc[:, ['integration_id', 'course_id', 'course_name',
                'default_credit_hours', 'Level', 'description', 'status',
                'catalog_year', ]]

df = df.sort_values(['integration_id'])

today = datetime.now().strftime('%Y%m%d')
fn_output = f'{today}_course_catalog.txt'
df.to_csv(fn_output, index=False)
