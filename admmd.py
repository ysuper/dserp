from connect_db import SqlSrv
from config import db_cfg
import pandas as pd
import os


class ADMMD(SqlSrv):
    """
    ADMMD
    欄位資料
    """

    def __init__(self, sql_db_cfg, table_name):
        super().__init__(sql_db_cfg)
        cache_path = 'dserp/cache' if os.path.exists('dserp') else 'cache'
        if not os.path.exists(cache_path):
            os.mkdir(cache_path)
        self.table_name = table_name
        self.cache_file = "{}/{}.json".format(cache_path, self.table_name)
        self.get_df()

    def get_df(self):
        if os.path.isfile(self.cache_file):
            self.get_df_by_cache()
        else:
            self.get_df_by_sql()

    def get_df_by_cache(self):
        self.df = pd.read_json(self.cache_file, orient='split', encoding="utf-8")

    def get_df_by_sql(self):
        sql = """
        SELECT MD001 '檔案代碼',
               MD003 '欄位編號',
               MD004 '欄位名稱',
               MD007 '欄位說明'
        FROM   DSCSYS.dbo.ADMMD
        WHERE  MD001 = '{}'
        """.format(self.table_name)
        self.sql_query(sql)
        self.df = self.export_df()
        self.write_cache()

    def write_cache(self):
        self.export_json(self.cache_file)


if __name__ == "__main__":
    table_name = 'COPMA'
    admmd = ADMMD(db_cfg, table_name)
    print(admmd.df)
