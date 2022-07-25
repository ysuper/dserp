from connect_db import SqlSrv
from config import db_cfg
from .common import COMMON
from .admmd import ADMMD
import pandas as pd
import os
import re


class TABLE(SqlSrv, COMMON):
    """
    TABLE
    客戶基本資料檔
    """

    def __init__(self, sql_db_cfg, tbl_name):
        SqlSrv.__init__(self, sql_db_cfg)
        COMMON.__init__(self, table_name=tbl_name)
        sql = "SELECT * FROM {}".format(tbl_name)
        self.sql_query(sql)
        self.df = self.export_df()
        self.df.index += 1
        # self.df['MA001'] = self.df['MA001'].str.strip()


if __name__ == "__main__":
    table = TABLE(db_cfg, "INVMB")
    columns = ['MB001', 'MB002', 'MB003']
    table.df = table.get_columns(columns).head(10)
    # table.df.columns = table.translate()
    table.df.columns = table.translate(columns)
    # print(table.translate)
    print(table.df)
    # table.export_html("table.html")
