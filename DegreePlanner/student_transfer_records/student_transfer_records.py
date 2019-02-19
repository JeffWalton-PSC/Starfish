import numpy as np
import pandas as pd
from datetime import date
from pathlib import Path

output_path = Path(
    r"\\psc-data\E\Applications\Starfish\Files\workingfiles\student_transfer_records"
)
fn_output = output_path / "student_transfer_records.txt"

# local connection information
import local_db

connection = local_db.connection()

# utility functions
import util

today = date.today()
today_str = today.strftime("%Y%m%d")

sql_str = "SELECT * FROM TRANSCRIPTDETAIL WHERE " + "CREDIT_TYPE = 'TRAN' "
df_td = pd.read_sql_query(sql_str, connection)

df_td = df_td[
    [
        "PEOPLE_CODE_ID",
        "ACADEMIC_YEAR",
        "ACADEMIC_TERM",
        "ACADEMIC_SESSION",
        "EVENT_ID",
        "EVENT_SUB_TYPE",
        "SECTION",
        "EVENT_MED_NAME",
        "ORG_CODE_ID",
        "CREDIT_TYPE",
        "CREDIT",
        "FINAL_GRADE",
        "REFERENCE_EVENT_ID",
        "REFERENCE_SUB_TYPE",
    ]
]

# keep transfer records for active students
df = util.apply_active(in_df=df_td)

crs_id = (
    lambda c: (str(c["EVENT_ID"]).replace(" ", "") + str(c["EVENT_SUB_TYPE"]).upper())
    if ((c["EVENT_SUB_TYPE"] == "LAB") | (c["EVENT_SUB_TYPE"] == "SI"))
    else (str(c["EVENT_ID"]).replace(" ", ""))
)
df.loc[:, "transfer_course_number"] = df.apply(crs_id, axis=1)

tr_section_id = (
    lambda c: (c["EVENT_ID"] + "." + c["EVENT_SUB_TYPE"] + ".Transfer")
    if ((c["ACADEMIC_YEAR"] == "1999") | (c["ACADEMIC_YEAR"] == "2004"))
    else (
        c["EVENT_ID"]
        + "."
        + c["EVENT_SUB_TYPE"]
        + "."
        + c["ACADEMIC_YEAR"]
        + "."
        + c["ACADEMIC_TERM"].title()
        + ".TR"
    )
)
df.loc[:, "transfer_course_section_number"] = df.apply(tr_section_id, axis=1)
df.loc[:, "ag_grading_type"] = "P/F"
df.loc[:, "ag_status"] = "TRANSFER"

df = df.rename(
    columns={
        "PEOPLE_CODE_ID": "student_integration_id",
        "CREDIT": "credits",
        "EVENT_MED_NAME": "course_title",
        "ACADEMIC_YEAR": "term_year",
        "ACADEMIC_TERM": "term_season",
    }
)

tr_grade = lambda c: "P" if (c["FINAL_GRADE"] == "TR") else "NG"
df.loc[:, "ag_grade"] = df.apply(tr_grade, axis=1)
df = df[~df["ag_grade"].isnull()]

df = df.loc[
    :,
    [
        "student_integration_id",
        "transfer_course_number",
        "transfer_course_section_number",
        "ag_grade",
        "ag_grading_type",
        "ag_status",
        "credits",
        "course_title",
    ],
]

df = df.sort_values(
    ["student_integration_id", "transfer_course_section_number"]
).drop_duplicates(
    ["student_integration_id", "transfer_course_section_number"], keep="last"
)

df.to_csv(fn_output, index=False)
