from connect_db import SqlSrv
from config import db_cfg


class EstimatedIncomingShipments(SqlSrv):
    """
    Estimated incoming shipments
    預計進貨
    """

    def __init__(self, sql_db_cfg):
        super().__init__(sql_db_cfg)
        sql = """
        SELECT
        TD004 '品號'
        , SUM(TD008-TD015) '預計進貨'
        FROM PURTD -- 採購單單身資料檔
        WHERE TD018 = 'Y' -- 確認碼 (Y/N/V)
        AND TD001 IN ('331', '332')
        AND CONVERT(DATETIME, CREATE_DATE) >= DATEADD(YEAR, -1, GETDATE())
        GROUP BY TD004
        """

        self.sql_query(sql)
        self.df = self.export_df()

        # Remove unnecessary spaces in the string
        self.df["品號"] = self.df["品號"].str.strip()


if __name__ == "__main__":
    estimated_incoming_shipments = EstimatedIncomingShipments(db_cfg)
    print(estimated_incoming_shipments.df)
