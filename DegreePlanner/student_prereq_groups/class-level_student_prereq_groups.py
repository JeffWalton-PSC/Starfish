"""
Data file for Starfish's DegreePlanner.
Creates pre-requisite groups based on class level (Freshman, Sophomore, Junior, Senior).
"""
import pandas as pd
from datetime import date
from pathlib import Path

output_path = Path(r'\\psc-data\E\Applications\Starfish\Files\workingfiles\student_prereq_groups')
fn_output = output_path / 'class-level_student_prereq_groups.txt'

# local connection information
import local_db
connection = local_db.connection()

# utility functions
import util

today = date.today()
today_str = today.strftime('%Y%m%d')


# find the latest year_term
def latest_year_term(df):
    """
    Return df with most recent records based on ACADEMIC_YEAR andACADEMIC_TERM
    """
    df = df.copy()
    df = df[(df['ACADEMIC_YEAR'].notnull()) & (df['ACADEMIC_YEAR'].str.isnumeric())]
    df['ACADEMIC_YEAR'] = (pd.to_numeric(df['ACADEMIC_YEAR'], errors='coerce'))
    df_seq = pd.DataFrame([
                           {'term': 'Transfer', 'seq': 0},
                           {'term': 'SPRING', 'seq': 1},
                           {'term': 'SUMMER', 'seq': 2},
                           {'term': 'FALL', 'seq': 3}])
    df = pd.merge(df, df_seq, left_on='ACADEMIC_TERM', right_on='term', how='left')
    df['term_seq'] = df['ACADEMIC_YEAR'] * 100 + df['seq']

    d=df.reset_index().groupby(['PEOPLE_CODE_ID'])['term_seq'].idxmax()
    
    df = df.loc[df.reset_index().groupby(['PEOPLE_CODE_ID'])['term_seq'].idxmax()]

    return df


sql_str = "SELECT PEOPLE_CODE_ID, ACADEMIC_YEAR, ACADEMIC_TERM, ACADEMIC_SESSION, " + \
          "CREDITS, PRIMARY_FLAG, CLASS_LEVEL " + \
          "FROM ACADEMIC WHERE " + \
          "ACADEMIC_SESSION = '' " + \
          "AND PRIMARY_FLAG = 'Y' " + \
          "AND CREDITS > 0 "
df_aca = pd.read_sql_query(sql_str, connection)

df_aca = df_aca[['PEOPLE_CODE_ID', 'ACADEMIC_YEAR', 'ACADEMIC_TERM', 'ACADEMIC_SESSION', 
                 'CREDITS', 'PRIMARY_FLAG', 'CLASS_LEVEL', 
                 ]]

df_aca = latest_year_term(df_aca)

sql_str = "SELECT PEOPLE_CODE_ID, ACADEMIC_YEAR, ACADEMIC_TERM, ACADEMIC_SESSION, " + \
          "RECORD_TYPE, TOTAL_CREDITS, GPA " + \
          "FROM TRANSCRIPTGPA WHERE " + \
          "RECORD_TYPE = 'O' " + \
          "AND TOTAL_CREDITS >= 0 "
df_tgpa = pd.read_sql_query(sql_str, connection)

df_tgpa = df_tgpa[['PEOPLE_CODE_ID', 'ACADEMIC_YEAR', 'ACADEMIC_TERM', 'ACADEMIC_SESSION', 
                 'RECORD_TYPE', 'TOTAL_CREDITS',
                 ]]

df_tgpa = latest_year_term(df_tgpa)

df = pd.merge(df_aca, df_tgpa, 
              on=['PEOPLE_CODE_ID'],
              how='left')

# keep records for active students
df = util.apply_active(in_df=df)

df.loc[:,'prereq_group_identifier'] = 'FRESHMAN'
df.loc[(df['TOTAL_CREDITS'] >= 30),'prereq_group_identifier'] = 'SOPHOMORE'
df.loc[(df['TOTAL_CREDITS'] >= 60),'prereq_group_identifier'] = 'JUNIOR'
df.loc[(df['TOTAL_CREDITS'] >= 90),'prereq_group_identifier'] = 'SENIOR'

df = df.rename(columns={
                        'PEOPLE_CODE_ID': 'student_integration_id',
                       })


df = df.loc[:, ['student_integration_id', 'prereq_group_identifier',
               ]]

df = (df.sort_values(['student_integration_id', 
                      'prereq_group_identifier'])
        .drop_duplicates(['student_integration_id', 
                          'prereq_group_identifier'],
                         keep='last')
     )

df.to_csv(fn_output, index=False)
