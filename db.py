import sqlite3
from pathlib import Path

DB_PATH = Path("DATA") / "intelligence_platform.db"

def connect_database(db_path=DB_PATH):
    """Connect to SQLite database."""
    return sqlite3.connect(str(db_path))

import pandas as pd
from pathlib import Path

def load_csv_to_table(conn, csv_path, table_name):
    path = Path(csv_path)
    if not path.exists():
        return 0
    
    df = pd.read_csv(path)
    df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
    return len(df)