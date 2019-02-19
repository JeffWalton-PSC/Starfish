import numpy as np
import pandas as pd
from datetime import date
from pathlib import Path

output_path = Path(
    r"\\psc-data\E\Applications\Starfish\Files\workingfiles\section_schedules"
)
fn_output = output_path / "section_schedules.txt"

# local connection information
import local_db

connection = local_db.connection()

today = date.today()
today_str = today.strftime("%Y%m%d")

sections_begin_year = "2011"

sql_str = (
    "SELECT * FROM SECTIONSCHEDULE WHERE "
    + f"ACADEMIC_YEAR >= '{sections_begin_year}' "
    + "AND ACADEMIC_TERM IN ('FALL', 'SPRING', 'SUMMER') "
    + "AND ACADEMIC_SESSION IN ('MAIN', 'CULN', 'EXT', 'FNRR', 'HEOP', "
    + " 'SLAB', 'BLOCK A', 'BLOCK AB', 'BLOCK B') "
)
df_ss = pd.read_sql_query(sql_str, connection)

df = df_ss[
    [
        "ACADEMIC_YEAR",
        "ACADEMIC_TERM",
        "ACADEMIC_SESSION",
        "EVENT_ID",
        "EVENT_SUB_TYPE",
        "SECTION",
        "DAY",
        "START_TIME",
        "END_TIME",
        "BUILDING_CODE",
        "ROOM_ID",
    ]
]

df = df[~(df["EVENT_ID"].str.contains("REG", case=False))]
df = df[~(df["EVENT_ID"].str.contains("STDY", case=False))]

df = df.loc[
    (~df["EVENT_SUB_TYPE"].isin(["ACE", "EXT", "ONLN"]))
    & (~df["DAY"].isin(["TBD", "ONLN", "CANC"]))
    & (~df["BUILDING_CODE"].isin(["ONLINE"]))
    & (~df["BUILDING_CODE"].isnull())
]

df.loc[:, "section_integration_id"] = (
    df["EVENT_ID"]
    + "."
    + df["EVENT_SUB_TYPE"]
    + "."
    + df["ACADEMIC_YEAR"]
    + "."
    + df["ACADEMIC_TERM"].str.title()
    + "."
    + df["SECTION"]
)

# building codes
sql_str = "SELECT BUILDING_CODE, BUILD_NAME_1 FROM BUILDING "
building_codes = pd.read_sql_query(sql_str, connection)
df = pd.merge(df, building_codes, on=["BUILDING_CODE"], how="left")
df = df.rename(columns={"BUILD_NAME_1": "building", "ROOM_ID": "room"})

df["start_time"] = df.START_TIME.dt.strftime("%I:%M%p")
df["end_time"] = df.END_TIME.dt.strftime("%I:%M%p")

# day codes
sql_str = "SELECT CODE_VALUE, DAY_SORT FROM CODE_DAY "
day_codes = pd.read_sql_query(sql_str, connection)
day_func = lambda c: (
    str(c["DAY_SORT"])
    .replace("1", "M")
    .replace("2", "T")
    .replace("3", "W")
    .replace("4", "R")
    .replace("5", "F")
    .replace("6", "A")
    .replace("7", "S")
)
day_codes.loc[:, "meeting_days"] = day_codes.apply(day_func, axis=1)
df = pd.merge(df, day_codes, left_on=["DAY"], right_on=["CODE_VALUE"], how="left")

df = df.loc[
    :,
    [
        "section_integration_id",
        "meeting_days",
        "start_time",
        "end_time",
        "building",
        "room",
    ],
]

df = df.sort_values(
    ["section_integration_id", "meeting_days", "start_time"]
).drop_duplicates(["section_integration_id", "meeting_days", "start_time"], keep="last")

df.to_csv(fn_output, index=False)
