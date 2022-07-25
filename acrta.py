from connect_db import SqlSrv
from config import db_cfg
import os
try:
    from .common import COMMON
except Exception:
    from common import COMMON


class ACRTA(SqlSrv, COMMON):
    """
    ACRTA
    結帳單單頭檔
    """

    def __init__(self, sql_db_cfg):
        table_name = os.path.basename(__file__).split('.')[0].upper()
        SqlSrv.__init__(self, sql_db_cfg)
        COMMON.__init__(self, table_name=table_name)
        sql = """
        SELECT * FROM ACRTA
        """
        self.sql_query(sql)
        self.df = self.export_df()
        self.df.index += 1
        self.df['TA001'] = self.df['TA001'].str.strip()


if __name__ == "__main__":
    table = ACRTA(db_cfg)
    columns = ['TA001', 'TA002', 'TA003']
    table.df = table.get_columns(columns).head(10)
    # table.df.columns = table.translate()
    table.df.columns = table.translate(columns)
    print(table.translate)
    print(table.df)
    table.export_html("table.html")
