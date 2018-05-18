import numpy as np
import pandas as pd
from datetime import date, datetime

# local connection information
import local_db
connection = local_db.connection()

sections_begin_year = '2011'

sql_str = "SELECT * FROM EVENT WHERE " + \
          "EVENT_STATUS = 'A' " + \
          "AND EVENT_TYPE = 'CLAS' "
df_event = pd.read_sql_query(sql_str, connection)

dfe = df_event[['EVENT_ID', 'EVENT_LONG_NAME', 'DESCRIPTION', ]]

dfe = dfe.rename(columns={'EVENT_LONG_NAME': 'course_name',
                          'DESCRIPTION': 'description'})

sql_str = "SELECT * FROM SECTIONS WHERE " + \
          "EVENT_SUB_TYPE NOT IN ('ADV') " + \
          f"AND ACADEMIC_YEAR >= '{sections_begin_year}' " + \
          "AND ACADEMIC_TERM IN ('FALL', 'SPRING', 'SUMMER') " + \
          "AND ACADEMIC_SESSION IN ('MAIN', 'CULN', 'EXT', 'FNRR', 'HEOP'," + \
          " 'SLAB', 'BLOCK A', 'BLOCK AB', 'BLOCK B') "
df_sections = pd.read_sql_query(sql_str, connection)

dfs = df_sections[['EVENT_ID', 'EVENT_SUB_TYPE', 'SECTION', 'ACADEMIC_YEAR',
                   'ACADEMIC_TERM', 'ACADEMIC_SESSION', 'CIP_CODE', 'CREDITS',
                   'REVISION_DATE', 'REVISION_TIME',
                   ]]

dfs = dfs.sort_values(['EVENT_ID', 'EVENT_SUB_TYPE', 'ACADEMIC_YEAR',
                       'ACADEMIC_TERM', 'SECTION'],
                      ascending=[True, True, True, False, True])
dfs = dfs.drop_duplicates(['EVENT_ID', 'EVENT_SUB_TYPE', 'ACADEMIC_YEAR',
                           'ACADEMIC_TERM', 'SECTION',
                           ],
                          keep='first')

dfcat = dfs.sort_values(['EVENT_ID', 'EVENT_SUB_TYPE', 'CREDITS',
                         'ACADEMIC_YEAR', 'ACADEMIC_TERM'],
                        ascending=[True, True, True, True, False])
dfcat = dfcat.drop_duplicates(['EVENT_ID', 'EVENT_SUB_TYPE', 'CREDITS'],
                              keep='first')

dfcat = dfcat.rename(columns={'CREDITS': 'default_credit_hours',
                              'CIP_CODE': 'course_cip_code'})

df = pd.merge(dfe, dfcat, on=['EVENT_ID'], how='left')

df = df[~(df['EVENT_ID'].str.contains('REG', case=False))]
df = df[~(df['EVENT_ID'].str.contains('STDY', case=False))]
df = df[~(df['EVENT_ID'].str.contains('PRV TRAN', case=False))]


df.loc[:, 'Level'] = '0' + df.loc[:, 'EVENT_ID'].str[-3:-2]
df.loc[:, 'Level'] = df.loc[:, 'Level'].str.replace(' ', '')  # for MAT 98

crs_id = (lambda c: (str(c['EVENT_ID']).replace(' ', '') +
                     str(c['EVENT_SUB_TYPE']).upper())
          if ((c['EVENT_SUB_TYPE'] == 'LAB') | (c['EVENT_SUB_TYPE'] == 'SI'))
          else (str(c['EVENT_ID']).replace(' ', ''))
          )
df.loc[:, 'course_id'] = df.apply(crs_id, axis=1)

df.loc[:, 'description'] = (df.loc[:, 'description'].str.replace('\n', ' ')
                                                    .str.replace('\r', ' ')
                                                    .str.replace('\"\"', "\'")
                                                    .str.replace('\"', "\'")
                            )

df.loc[:, 'status'] = 'ACTIVE'

df['ACADEMIC_YEAR'] = (pd.to_numeric(df['ACADEMIC_YEAR'], errors='coerce')
                         .fillna(sections_begin_year).astype(np.int64))

df.loc[:, 'EVENT_SUB_TYPE'] = df.loc[:, 'EVENT_SUB_TYPE'].fillna('')
df.loc[:, 'ACADEMIC_YEAR'] = (df.loc[:, 'ACADEMIC_YEAR']
                                .fillna(sections_begin_year))
df.loc[:, 'ACADEMIC_TERM'] = df.loc[:, 'ACADEMIC_TERM'].fillna('FALL')

cat_yr = (lambda c: c['ACADEMIC_YEAR'] if (c['ACADEMIC_TERM'] == 'FALL')
          else (c['ACADEMIC_YEAR'] - 1))
df.loc[:, 'catalog_year'] = df.apply(cat_yr, axis=1)

integ_id = (lambda c: (c['EVENT_ID'] + '.' + str(c['catalog_year']))
            if (c['EVENT_SUB_TYPE'] == '')
            else (c['EVENT_ID'] + '.' + c['EVENT_SUB_TYPE'] + '.' +
                  str(c['catalog_year'])))
df.loc[:, 'integration_id'] = df.apply(integ_id, axis=1)

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
