from connect_db import SqlSrv
from config import db_cfg
import os
try:
    from .common import COMMON
except Exception:
    from common import COMMON


class ACRTC(SqlSrv, COMMON):
    """
    ACRTC
    收款單單頭檔
    """

    def __init__(self, sql_db_cfg):
        table_name = os.path.basename(__file__).split('.')[0].upper()
        SqlSrv.__init__(self, sql_db_cfg)
        COMMON.__init__(self, table_name=table_name)
        sql = """
        SELECT * FROM ACRTC
        """
        self.sql_query(sql)
        self.df = self.export_df()
        self.df.index += 1
        self.df['TC001'] = self.df['TC001'].str.strip()


if __name__ == "__main__":
    table = ACRTC(db_cfg)
    columns = ['TC001', 'TC002', 'TC003']
    table.df = table.get_columns(columns).head(10)
    # table.df.columns = table.translate()
    table.df.columns = table.translate(columns)
    print(table.translate)
    print(table.df)
    table.export_html("table.html")
