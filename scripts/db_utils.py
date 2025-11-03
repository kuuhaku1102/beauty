# beauty/scripts/db_utils.py
import os
from sqlalchemy import create_engine

def get_engine():
    """
    SSHトンネル経由でMySQLへ接続する
    （GitHub Actionsで localhost:3307 にポートフォワード済み）
    """
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME")
    host = "127.0.0.1"
    port = 3307  # SSHトンネルでフォワード済み

    url = f"mysql+pymysql://{db_user}:{db_pass}@{host}:{port}/{db_name}?charset=utf8mb4"
    return create_engine(url, echo=False)
