import pandas as pd
from datetime import date
import local_db

# create active student list from 2-year rolling window
def active_students():
    """
    returns DataFrame of active student IDs

    Active Students are those that have been enrolled in last two years.
    """

    connection = local_db.connection()

    today = date.today()
    two_years_ago = today.year - 2
    sql_str = "SELECT PEOPLE_CODE_ID FROM ACADEMIC WHERE " + \
            f"ACADEMIC_YEAR > '{two_years_ago}' " + \
            "AND PRIMARY_FLAG = 'Y' " + \
            "AND CURRICULUM NOT IN ('ADVST') " + \
            "AND GRADUATED NOT IN ('G') "
    active = pd.read_sql_query(sql_str, connection)
    active = active.drop_duplicates(['PEOPLE_CODE_ID'])

    return active

def apply_active(in_df):
    """
    returns copy of in_df with only records for active students

    in_df is an input DataFrame, must have PEOPLE_CODE_ID field
    """

    active = active_students()

    # return records for active students
    df = pd.merge(in_df, active, how='inner', on='PEOPLE_CODE_ID')

    return df