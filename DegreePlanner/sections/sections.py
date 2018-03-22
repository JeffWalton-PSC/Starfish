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

sql_str = "SELECT * FROM SECTIONS WHERE " + \
          "EVENT_SUB_TYPE NOT IN ('ADV') " + \
          f"AND ACADEMIC_YEAR >= '{sections_begin_year}' " + \
          "AND ACADEMIC_TERM IN ('FALL', 'SPRING', 'SUMMER') " + \
          "AND ACADEMIC_SESSION IN ('MAIN', 'CULN', 'EXT', 'FNRR', 'HEOP'," + \
          " 'SLAB', 'BLOCK A', 'BLOCK AB', 'BLOCK B') "
df_sections = pd.read_sql_query(sql_str, connection)

df = df_sections[['EVENT_ID', 'EVENT_SUB_TYPE', 'EVENT_MED_NAME',
                  'SECTION', 'CREDITS', 'MAX_PARTICIPANT',
                  'ACADEMIC_YEAR', 'ACADEMIC_TERM', 'ACADEMIC_SESSION',
                  'START_DATE', 'END_DATE', 'CIP_CODE',
                  'REVISION_DATE', 'REVISION_TIME',
                  ]]

df = df[~(df['EVENT_ID'].str.contains('REG', case=False))]
df = df[~(df['EVENT_ID'].str.contains('STDY', case=False))]

df = df.rename(columns={'EVENT_MED_NAME': 'course_section_name',
                        'CREDITS': 'credit_hours',
                        'MAX_PARTICIPANT': 'maximum_enrollment_count',
                        'START_DATE': 'start_dt',
                        'END_DATE': 'end_dt',
                        'CIP_CODE': 'course_cip_code',
                        })

df.loc[:, 'course_id'] = df.loc[:, 'EVENT_ID'].str.replace(' ', '')

df.loc[:, 'course_section_id'] = (df['EVENT_ID'] + '.' +
                                  df['EVENT_SUB_TYPE'] + '.' +
                                  df['ACADEMIC_YEAR'] + '.' +
                                  df['ACADEMIC_TERM'].str.title() + '.' +
                                  df['SECTION']
                                  )
df.loc[:, 'integration_id'] = df.loc[:, 'course_section_id']

term_id = (lambda c: (c['ACADEMIC_YEAR'] + '.' +
                      str(c['ACADEMIC_TERM']).title())
           if (c['ACADEMIC_SESSION'] == 'MAIN')
           else (c['ACADEMIC_YEAR'] + '.' +
                 str(c['ACADEMIC_TERM']).title() + '.' +
                 c['ACADEMIC_SESSION'])
           )
df.loc[:, 'term_id'] = df.apply(term_id, axis=1)

df['AY'] = (pd.to_numeric(df['ACADEMIC_YEAR'], errors='coerce')
              .fillna(sections_begin_year).astype(np.int64))
cat_yr = (lambda c: c['AY'] if (c['ACADEMIC_TERM'] == 'FALL')
          else (c['AY'] - 1))
df.loc[:, 'catalog_year'] = df.apply(cat_yr, axis=1)

crs_sect_delv = (lambda c: '03'
                 if str(c['SECTION'])[:2] == 'HY'
                 else ('02' if str(c['SECTION'])[:2] == 'ON'
                       else '01')
                 )
df.loc[:, 'course_section_delivery'] = df.apply(crs_sect_delv, axis=1)

# read course_catalog.txt to find the correct catalog year
dfcat = pd.read_csv('../course_catalog/course_catalog.txt')


crs_integ_id = (lambda c: (c['EVENT_ID'] + '.' + str(c['catalog_year']))
                if (c['EVENT_SUB_TYPE'] == '')
                else (c['EVENT_ID'] + '.' + c['EVENT_SUB_TYPE'] + '.' +
                      str(c['catalog_year'])))
df.loc[:, 'course_integration_id'] =

df = df.loc[:, ['integration_id', 'course_section_name', 'course_section_id',
                'start_dt', 'end_dt', 'term_id', 'course_integration_id',
                'course_section_delivery', 'maximum_enrollment_count',
                'credit_hours',
                ]]

df = df.sort_values(['integration_id'])

today = datetime.now().strftime('%Y%m%d')
fn_output = f'{today}_sections.txt'
df.to_csv(fn_output, index=False)
