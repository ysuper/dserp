from pypika import AliasedQuery, Criterion, Query, Tables
from pypika import functions as fn

from config import db_cfg
from connect_db import SqlSrv


class InventoryQuantity(SqlSrv):
    """
    Inventory Quantity
    庫存數量
    """

    def __init__(self, sql_db_cfg):
        super().__init__(sql_db_cfg)
        sql = self.generate_sql()

        self.sql_query(sql)
        self.df = self.export_df()
        # print(self.df)

        # Remove unnecessary spaces in the string
        self.df["品號"] = self.df["品號"].str.strip()

    def generate_sql(self):

        mylist = [
            "C-Z00001",
            "EW7100J42V10",
            "EANC6000MT2125",
            "EANC6000A00159",
            "3TP000A0000040",
            "MPNC5100010200",
            "EANC7100BLD122",
            "3TP00008030040",
            "EANC6000S00132",
            "EANC7101A00137",
        ]

        invmb, invmc, cmsmc = Tables("INVMB", "INVMC", "CMSMC")

        sub_query = (
            Query.from_(invmb)
            .left_join(invmc)
            .on(invmb.MB001 == invmc.MC001)
            .left_join(cmsmc)
            .on(invmc.MC002 == cmsmc.MC001)
            # .select(invmb.star)
            .select(
                invmb.MB001.as_("品號"),
                invmb.MB002.as_("品名"),
                invmc.MC009.as_("標準存貨量"),
                invmc.MC007.as_("庫存數量"),
                invmc.MC004.as_("安全存量"),
                invmc.MC002,
            )
            .where(
                Criterion.all(
                    [
                        invmb.MB017.notin(["A08"]),
                        invmb.MB001.notin(mylist),
                    ]
                )
            )
        )

        an_alias = AliasedQuery("an_alias")
        q = (
            Query.with_(sub_query, "an_alias")
            .from_(an_alias)
            .select(an_alias.品號)
            .select(an_alias.品名)
            .select(fn.Sum(an_alias.庫存數量).as_("庫存數量"))
            # .select("*")
            .where(an_alias.MC002.isin(["A01", "A04", "B01", "B06"]))
            .groupby(an_alias.品號, an_alias.品名)
        )

        str(q)

        sql = q.get_sql()
        # print(sql)
        return sql

    def print_sql(self):
        print(self.generate_sql())


if __name__ == "__main__":
    inventory_quantity = InventoryQuantity(db_cfg)
    # inventory_quantity.print_sql()
    print(inventory_quantity.df)
