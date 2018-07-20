import numpy as np
import pandas as pd
from datetime import date
from pathlib import Path

output_path = Path(r'\\psc-data\E\Applications\Starfish\Files\workingfiles\student_test_results')
fn_output = output_path / 'student_test_results.txt'

# local connection information
import local_db
connection = local_db.connection()

# utility functions
import util

today = date.today()
today_str = today.strftime('%Y%m%d')

sql_str = "SELECT PEOPLE_CODE_ID, TEST_ID, TEST_TYPE, " + \
          "CONVERTED_SCORE, TEST_DATE " + \
          "FROM TESTSCORES WHERE " + \
          "TEST_ID = 'ACC' " + \
          "AND ( TEST_TYPE = 'MATH' " + \
          "OR TEST_TYPE = 'ENGL' ) "
df = pd.read_sql_query(sql_str, connection, parse_dates=['TEST_DATE'])

df  = df[df['TEST_DATE'].notnull()]

df.loc[(df['TEST_TYPE'] == 'MATH'), 'test_id'] = 'ACCUPLACER_MATH'
df.loc[(df['TEST_TYPE'] == 'ENGL'), 'test_id'] = 'ACCUPLACER_ENGLISH'

df['numeric_score'] = df['CONVERTED_SCORE'].dropna().apply(np.int64)

df['date_taken'] = df['TEST_DATE'].dt.strftime('%Y-%m-%d')

# keep records for active students
df = util.apply_active(in_df=df)

df = df.rename(columns={
                        'PEOPLE_CODE_ID': 'student_integration_id',
                       })

df = df.loc[:, ['student_integration_id', 'test_id',
                'numeric_score', 'date_taken', 
               ]]

df = (df.sort_values(['student_integration_id', 
                      'test_id', 'numeric_score'])
        .drop_duplicates(['student_integration_id', 
                          'test_id'],
                         keep='last')
     )

df.to_csv(fn_output, index=False)
