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
    sql_str = (
        "SELECT PEOPLE_CODE_ID FROM ACADEMIC WHERE "
        + f"ACADEMIC_YEAR > '{two_years_ago}' "
        + "AND PRIMARY_FLAG = 'Y' "
        + "AND CURRICULUM NOT IN ('ADVST') "
        + "AND GRADUATED NOT IN ('G') "
    )
    active = pd.read_sql_query(sql_str, connection)
    active = active.drop_duplicates(["PEOPLE_CODE_ID"])

    return active


# create user list of PEOPLE_CODE_ID's with HOME email_addresses
def with_email_address():
    """
    returns DataFrame of PEOPLE_CODE_ID's with non-NULL HOME email_addresses

    """

    connection = local_db.connection()

    sql_str = (
        "SELECT PEOPLE_ORG_CODE_ID FROM ADDRESS WHERE "
        + "ADDRESS_TYPE = 'HOME' "
        + "AND EMAIL_ADDRESS IS NOT NULL "
        + "AND EMAIL_ADDRESS LIKE '%@%' "
    )
    with_address = pd.read_sql_query(sql_str, connection)
    with_address = with_address.drop_duplicates(["PEOPLE_ORG_CODE_ID"])

    with_address = with_address.rename(columns={"PEOPLE_ORG_CODE_ID": "PEOPLE_CODE_ID"})

    return with_address


def apply_active(in_df):
    """
    returns copy of in_df with only records for active students

    in_df is an input DataFrame, must have PEOPLE_CODE_ID field
    """

    active = active_students()

    # return records for active students
    df = pd.merge(in_df, active, how="inner", on="PEOPLE_CODE_ID")

    return df


def apply_active_with_email_address(in_df):
    """
    returns copy of in_df with only records for active students with email_address

    in_df is an input DataFrame, must have PEOPLE_CODE_ID field
    """

    # keep records for active students
    active = apply_active(in_df=in_df)

    with_email = with_email_address()

    # return records for active students with email_address
    df = pd.merge(active, with_email, how="inner", on="PEOPLE_CODE_ID")

    return df


# find the latest year_term
def latest_year_term(df):
    """
    Return df with most recent records based on ACADEMIC_YEAR and ACADEMIC_TERM
    """
    df = df.copy()
    df = df[(df["ACADEMIC_YEAR"].notnull()) & (df["ACADEMIC_YEAR"].str.isnumeric())]
    df["ACADEMIC_YEAR"] = pd.to_numeric(df["ACADEMIC_YEAR"], errors="coerce")
    df_seq = pd.DataFrame(
        [
            {"term": "Transfer", "seq": 0},
            {"term": "SPRING", "seq": 1},
            {"term": "SUMMER", "seq": 2},
            {"term": "FALL", "seq": 3},
        ]
    )
    df = pd.merge(df, df_seq, left_on="ACADEMIC_TERM", right_on="term", how="left")
    df["term_seq"] = df["ACADEMIC_YEAR"] * 100 + df["seq"]

    #d = df.reset_index().groupby(["PEOPLE_CODE_ID"])["term_seq"].idxmax()

    df = df.loc[df.reset_index().groupby(["PEOPLE_CODE_ID"])["term_seq"].idxmax()]

    return df

