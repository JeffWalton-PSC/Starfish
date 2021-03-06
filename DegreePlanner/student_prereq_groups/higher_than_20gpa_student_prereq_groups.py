"""
Outputs group file for students with greater than a 2.0 GPA
"""

import pandas as pd
from datetime import date
from pathlib import Path

output_path = Path(
    r"\\psc-data\E\Applications\Starfish\Files\workingfiles\student_prereq_groups"
)
fn_output = output_path / "higherthan20gpa_student_prereq_groups.txt"

# local connection information
import local_db

connection = local_db.connection()

# utility functions
import util

today = date.today()
today_str = today.strftime("%Y%m%d")

sql_str = (
    "SELECT PEOPLE_CODE_ID, ACADEMIC_YEAR, ACADEMIC_TERM, ACADEMIC_SESSION, "
    + "RECORD_TYPE, TOTAL_CREDITS, GPA "
    + "FROM TRANSCRIPTGPA WHERE "
    + "RECORD_TYPE = 'O' "
    + "AND TOTAL_CREDITS >= 0 "
)
df_tgpa = pd.read_sql_query(sql_str, connection)


# keep records for active students with email_address
df = util.apply_active_with_email_address(in_df=df_tgpa)

# filter results to only have cumulative GPA's equal to or above a 2.0,
#                   and Fall, Spring or Summer term
df = df[(~df["GPA"].isnull())]
df = df[(df["GPA"] >= 2)]
df = df[df["ACADEMIC_TERM"].isin(["SPRING", "SUMMER", "FALL"])]

# find the latest year
df = util.latest_year_term(df)

# create prereq group identifier
df["prereq_group_identifier"] = "GPA_GT_2.0"

# rename columns
df = df.rename(columns={"PEOPLE_CODE_ID": "student_integration_id"})

# columns to keep
df = df[["student_integration_id", "prereq_group_identifier"]]

df = df.sort_values(
    ["student_integration_id", "prereq_group_identifier"]
).drop_duplicates(["student_integration_id", "prereq_group_identifier"], keep="last")

df.to_csv(fn_output, index=False)
