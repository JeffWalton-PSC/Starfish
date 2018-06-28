# Outputs group file for students with greater than a 2.0 GPA

import pandas as pd
import os
from datetime import date, datetime
from sqlalchemy import create_engine

begin_academic_year = '2011'

# local connection information
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
engine = create_engine(f'mssql+pyodbc://{db_user}:{db_pass}' +
                       '@PSC-SQLProd/Campus6?' +
                       'driver=ODBC+Driver+13+for+SQL+Server')
connection = engine.connect()

sql_str = """
SELECT [PEOPLE_CODE_ID]
      ,[ACADEMIC_YEAR]
      ,[ACADEMIC_TERM]
      ,[ACADEMIC_SESSION]
      ,[RECORD_TYPE]
      ,[GPA]
FROM [Campus6].[dbo].[TRANSCRIPTGPA]
"""
df = pd.read_sql_query(sql_str, connection)

df = df[df['ACADEMIC_YEAR'] >= begin_academic_year]

# Rename people_code_id to student_integration_id
df = df.rename(columns={'PEOPLE_CODE_ID': 'student_integration_id'})

# filter results to only have cumulative GPA's equal to or above a 2.0,
#                   have a record type of 'O' and Fall, Spring or Summer term
df = df[df['GPA'] >= 2]
df = df[df['RECORD_TYPE'] == 'O']
df = df[df['ACADEMIC_TERM'].isin(['SPRING', 'SUMMER', 'FALL'])]

# find the latest year
df['ACADEMIC_YEAR'] = (pd.to_numeric(df['ACADEMIC_YEAR'], errors='coerce'))
df_seq = pd.DataFrame([{'term': 'SPRING', 'seq': 1},
                       {'term': 'SUMMER', 'seq': 2},
                       {'term': 'FALL', 'seq': 3}])
df = pd.merge(df, df_seq, left_on='ACADEMIC_TERM', right_on='term', how='left')
df['term_seq'] = df['ACADEMIC_YEAR'] * 100 + df['seq']
df = (df.loc[df.reset_index()
               .groupby(['student_integration_id'])['term_seq']
               .idxmax()])

# create prereq group identifier
df['prereq_group_identifier'] = 'GPA_GT_2.0'

# columns to keep
df = df.loc[:, ['student_integration_id', 'prereq_group_identifier']]

df = df.drop_duplicates(['student_integration_id'], keep='first')

today = datetime.now().strftime('%Y%m%d')
fn_output = today + '_student_higherthan20gpa_student_prereq_groups.txt'
df.to_csv(fn_output, index=False)
