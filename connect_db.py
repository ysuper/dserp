from urllib.parse import quote_plus
from config import db_cfg
import pandas as pd
import records


class SqlSrv:

    def __init__(self, sql_db_cfg):
        self.url = ("mssql+pymssql://{username}:{password}@{server}:1433/{db_name}".format(
            username=sql_db_cfg.get("username"),
            server=sql_db_cfg.get("server"),
            password=sql_db_cfg.get("password"),
            db_name=sql_db_cfg.get("db_name"),
        ))
        self.db = records.Database(self.url)

    def test(self):
        rows = self.db.query("SELECT 1 + 1", fetchall=True)
        print(rows[0][0])

    def show_url(self):
        print(self.url)

    def sql_query(self, sql):
        self.rows = self.db.query(sql, fetchall=True)

    def export_df(self):
        data = self.rows.dataset.dict
        df = pd.DataFrame.from_dict(data)
        return df

    def export_html(self, filename):
        html = self.df.to_html()
        with open(filename, "w", encoding="utf-8") as file:
            file.write(html)

    def to_json(self):
        return self.df.to_json(orient="split")

    def export_json(self, filename):
        with open(filename, "w", encoding="utf-8") as file:
            file.write(self.df.to_json(orient="split", force_ascii=False))

    def export_excel(self, filename):
        self.df.to_excel(filename)


if __name__ == "__main__":
    sqlsrv = SqlSrv(db_cfg)
    sqlsrv.test()
