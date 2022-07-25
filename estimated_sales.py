from connect_db import SqlSrv
from config import db_cfg


class EstimatedSales(SqlSrv):
    """
    Estimated Sales
    預計銷貨
    """

    def __init__(self, sql_db_cfg):
        super().__init__(sql_db_cfg)
        sql = """
        SELECT
        TD004 '品號'
        , SUM(t2.TD008+t2.TD024-t2.TD009-t2.TD025) '預計銷貨'
        FROM COPTC t1 -- 客戶訂單單頭資料檔
        LEFT JOIN COPTD t2 ON t1.TC001 = t2.TD001 AND t1.TC002 = t2.TD002 -- 客戶訂單單身資料檔
        WHERE TC027 = 'Y' -- 確認碼 (Y:已確認、N:未確認、V:作廢)
        AND TD021 = 'Y' -- 確認碼 (Y/N/V)
        AND TD016 = 'N' -- 結案碼 (Y:自動結案、y:指定結案、N:未結案)
        AND TD013 <= DATEADD(month, 3, GETDATE()) -- 預交日
        GROUP BY TD004
        """

        self.sql_query(sql)
        self.df = self.export_df()

        # Remove unnecessary spaces in the string
        self.df["品號"] = self.df["品號"].str.strip()


if __name__ == "__main__":
    estimated_sales = EstimatedSales(db_cfg)
    print(estimated_sales.df)
