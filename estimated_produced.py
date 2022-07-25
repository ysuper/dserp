from connect_db import SqlSrv
from config import db_cfg


class EstimatedProduced(SqlSrv):
    """
    Estimated produced
    預計生產
    """

    def __init__(self, sql_db_cfg):
        super().__init__(sql_db_cfg)
        sql = """
        SELECT
        TA006 '品號'
        , SUM(TA015-TA017-TA018) "預計生產" -- 預計生產
        FROM MOCTA  -- 製造命令單頭檔
        WHERE TA011 IN ('1', '2', '3') -- 狀態碼 (1.未生產、2.已發料、3.生產中、Y.已完工、y.指定完工)
        AND TA001 IN ('511', '512') -- 製令單別
        AND TA013 = 'Y' -- 確認碼 (Y、N、V)
        GROUP BY TA006
        """
        self.sql_query(sql)
        self.df = self.export_df()

        # Remove unnecessary spaces in the string
        self.df["品號"] = self.df["品號"].str.strip()


if __name__ == "__main__":
    estimated_produced = EstimatedProduced(db_cfg)
    print(estimated_produced.df)
