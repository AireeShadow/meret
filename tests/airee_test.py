import sys
from pathlib import Path
sys.path[0] = str(Path(sys.path[0]).parent)

import sqlite3
from libs.database import Basic_Db_Operations



bdb = Basic_Db_Operations(location=".")
try:
    bdb.restore_db()
except sqlite3.OperationalError as ex:
    print(f'{ex}')