import numpy as np
import pandas as pd
from datetime import date

# local connection information
import local_db
connection = local_db.connection()

today = date.today()
today_str = today.strftime('%Y%m%d')

sql_str = """

SELECT [PEOPLE_CODE_ID] as student_integration_id, 'ACCUPLACER_MATH' as 'test_id', CONVERTED_SCORE as numeric_score, CONVERT(date, [TEST_DATE]) as date_taken
     
  FROM [Campus6].[dbo].[TESTSCORES]
  where TEST_ID = 'ACC'
  and TEST_TYPE = 'MATH' 

UNION
SELECT [PEOPLE_CODE_ID] as student_integration_id, 'ACCUPLACER_ENGLISH' as 'test_id', CONVERTED_SCORE as numeric_score, CONVERT(date, [TEST_DATE]) as date_taken
     
  FROM [Campus6].[dbo].[TESTSCORES]
  where TEST_ID = 'ACC'
  and TEST_TYPE = 'ENGL'


  order by student_integration_id


"""

df = pd.read_sql_query(sql_str, connection)



df['numeric_score'] = df['numeric_score'].dropna().apply(np.int64)

fn_output = f'{today_str}_student_test_results.txt'
df.to_csv(fn_output, index=False)
