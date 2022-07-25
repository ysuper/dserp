from connect_db import SqlSrv
from config import db_cfg


class EstimatedUsage(SqlSrv):
    """
    Estimated Usage
    預計領用
    """

    def __init__(self, sql_db_cfg):
        super().__init__(sql_db_cfg)
        sql = """
        SELECT
        TB003 '品號'
        , SUM(TB004-TB005) '預計領用'
        FROM MOCTA t1 -- 製造命令單頭檔
        LEFT JOIN MOCTB t2 ON t1.TA001 = t2.TB001 AND t1.TA002 = t2.TB002 -- 製造命令單身檔
        WHERE TB018 = 'Y' -- 確認碼 (Y/N/V)
        AND TA011 NOT IN ('Y', 'y') -- 狀態碼 (1.未生產、2.已發料、3.生產中、Y.已完工、y.指定完工)
        AND TB001 IN ('511', '512') -- 製令單別
        AND TB015 <= DATEADD(month, 3, GETDATE())
        GROUP BY TB003
        """

        self.sql_query(sql)
        self.df = self.export_df()
        self.df["品號"] = self.df["品號"].str.strip()


if __name__ == "__main__":
    estimated_usage = EstimatedUsage(db_cfg)
    print(estimated_usage.df)
