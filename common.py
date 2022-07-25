from config import db_cfg
import pandas as pd
try:
    from .admmd import ADMMD
except Exception:
    from admmd import ADMMD


class COMMON:
    """
    ERP Table 繼承模組
    """

    def __init__(self, table_name):
        self.table_name = table_name

    def get_columns(self, columns_list):
        return self.df.loc[:, columns_list]

    def translate(self, columns=None):
        translate_dict = {}
        admmd = ADMMD(db_cfg, self.table_name)
        columns = columns if columns else self.df.columns.values
        rtn_list = []
        for column in columns:
            if column in admmd.df['欄位編號'].str.strip().values:
                rtn_list.append(admmd.df.loc[admmd.df['欄位編號'].str.strip() == column, '欄位名稱'].values[0])
                translate_dict[column] = admmd.df.loc[admmd.df['欄位編號'].str.strip() == column, '欄位名稱'].values[0]
            else:
                rtn_list.append(column)
        self.translate = translate_dict
        return rtn_list

    @staticmethod
    def export_html(df, filename):
        html = df.to_html()
        with open(filename, "w", encoding="utf-8") as file:
            file.write(html)

    @staticmethod
    def object_to_float(df, column_list, number):
        decimal_place = "{:,." + str(number) + "f}"
        for column in column_list:
            df[column] = df[column].map(decimal_place.format)
        return df

    @staticmethod
    def df_to_mysql(dataFrame, url, tableName):
        from sqlalchemy import create_engine
        import pymysql
        import pandas as pd
        # url = 'mysql+pymysql://root:@127.0.0.1/test'
        # tableName   = "UserVitals"
        # dataFrame   = pd.DataFrame(data=userVitals)
        sqlEngine = create_engine(url, pool_recycle=3600)
        dbConnection = sqlEngine.connect()
        try:
            frame = dataFrame.to_sql(tableName, dbConnection, if_exists='replace')
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
        else:
            print("Table %s created successfully." % tableName)
        finally:
            dbConnection.close()
