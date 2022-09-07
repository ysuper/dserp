from config import db_cfg, mysql_url
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
        # decimal_place = "{:,." + str(number) + "f}"
        decimal_place = "{:." + str(number) + "f}"
        for column in column_list:
            df[column] = df[column].map(decimal_place.format)
        return df

    @staticmethod
    def object_to_date(df, column_list):
        for column in column_list:
            df[column] = pd.to_datetime(df[column]).dt.strftime('%Y-%m-%d')
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

    @staticmethod
    def export_excel(df, filename):
        df.to_excel(filename)


if __name__ == "__main__":
    students_data = pd.DataFrame({
        'Student': ['Samreena', 'Ali', 'Sara', 'Amna', 'Eva'],
        'marks': [800, 830, 740, 910, 1090],
        'Grades': ['B+', 'B+', 'B', 'A', 'A+']
    })

    ##### export_excel
    # file_name = 'table.xlsx'
    # COMMON.export_excel(students_data, file_name)

    ##### df_to_mysql
    tableName = "test"
    COMMON.df_to_mysql(students_data, mysql_url, tableName)
