import numpy as np
import pandas as pd
from datetime import date

# local connection information
import local_db
connection = local_db.connection()

today = date.today()
today_str = today.strftime('%Y%m%d')

sql_str = "SELECT * FROM TRANSCRIPTDETAIL WHERE " + \
          "ACADEMIC_YEAR = '1999' " + \
          "AND ACADEMIC_TERM = 'Transfer' "
df_td = pd.read_sql_query(sql_str, connection)

df_td = df_td[['PEOPLE_CODE_ID', 'ACADEMIC_YEAR', 'ACADEMIC_TERM', 'ACADEMIC_SESSION', 
             'EVENT_ID', 'EVENT_SUB_TYPE', 'SECTION', 'EVENT_MED_NAME', 
             'ORG_CODE_ID', 'CREDIT_TYPE', 'CREDIT', 'FINAL_GRADE', 
             'REFERENCE_EVENT_ID', 'REFERENCE_SUB_TYPE', 
             ]]

# create active student list from 2-year rolling window
two_years_ago = today.year - 2
sql_str = "SELECT PEOPLE_CODE_ID FROM ACADEMIC WHERE " + \
          f"ACADEMIC_YEAR > '{two_years_ago}' " + \
          "AND PRIMARY_FLAG = 'Y' " + \
          "AND CURRICULUM NOT IN ('ADVST') " + \
          "AND GRADUATED NOT IN ('G') "
active = pd.read_sql_query(sql_str, connection)
active = active.drop_duplicates(['PEOPLE_CODE_ID'])
# keep transfer records for active students
df = pd.merge(df_td, active, how='inner', on='PEOPLE_CODE_ID')

df = df.rename(columns={'PEOPLE_CODE_ID': 'student_integration_id',
                        'CREDIT': 'credits',
                        'FINAL_GRADE': 'ag_grade',
                       })

crs_id = (lambda c: (str(c['EVENT_ID']).replace(' ', '') +
                     str(c['EVENT_SUB_TYPE']).upper())
          if ((c['EVENT_SUB_TYPE'] == 'LAB') | (c['EVENT_SUB_TYPE'] == 'SI'))
          else (str(c['EVENT_ID']).replace(' ', ''))
          )
df.loc[:, 'transfer_course_number'] = df.apply(crs_id, axis=1)

df.loc[:, 'transfer_course_section_number'] = (df['EVENT_ID'] + '.' +
                                               df['EVENT_SUB_TYPE'] + '.' +
                                               'Transfer'
                                              )
df.loc[:, 'ag_grading_type'] = 'UNKNOWN'
df.loc[:, 'ag_status'] = 'TRANSFER'

df = df.loc[:, ['student_integration_id', 'transfer_course_number',
                'transfer_course_section_number',
                'ag_grade', 'ag_grading_type', 'ag_status',
                'credits', ]]

df = (df.sort_values(['student_integration_id', 
                      'transfer_course_section_number'])
        .drop_duplicates(['student_integration_id', 
                          'transfer_course_section_number'],
                         keep='last')
     )

fn_output = f'{today_str}_student_transfer_records.txt'
df.to_csv(fn_output, index=False)
