# beauty/scripts/db_writer.py
import pandas as pd
from beauty.scripts.db_utils import get_engine

def write_three_tables(clinics_rows, menus_rows, hours_rows):
    """
    3テーブルにデータを書き込む
    """
    engine = get_engine()
    with engine.begin() as conn:
        if clinics_rows:
            pd.DataFrame(clinics_rows).to_sql("clinics", conn, if_exists="append", index=False)
            print(f"[DB] clinics +{len(clinics_rows)} rows inserted")
        if menus_rows:
            pd.DataFrame(menus_rows).to_sql("menus", conn, if_exists="append", index=False)
            print(f"[DB] menus +{len(menus_rows)} rows inserted")
        if hours_rows:
            pd.DataFrame(hours_rows).to_sql("hours", conn, if_exists="append", index=False)
            print(f"[DB] hours +{len(hours_rows)} rows inserted")
