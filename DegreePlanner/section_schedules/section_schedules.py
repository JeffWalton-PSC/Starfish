import numpy as np
import pandas as pd
from datetime import date

# local connection information
import local_db
connection = local_db.connection()

today = date.today()
today_str = today.strftime('%Y%m%d')
print(today_str)

