class UpdateDatabase:
    def __init__(self, upload_engine, resource_engine, schema):
        self.schema = schema
        self.engine1 = upload_engine
        self.engine2 = resource_engine

    def update(self, table, index1: str, index2: str, chunk_size: int = 5000):
        import datetime

        postgres_datasize, mssql_datasize = self.get_data_size(table)

        # print(f"postgres_data: {postgres_datasize}, \n mssql_data :{mssql_datasize}")
        while (mssql_datasize - postgres_datasize) > 0:
            t0 = datetime.datetime.now()
            postgres_datasize, mssql_datasize = self.get_data_size(table)
            print(f"  Offset {postgres_datasize} rows fetch next {chunk_size} rows only")

            self.update_database(table, index1, index2, postgres_datasize, chunk_size)
            t1 = datetime.datetime.now()
            print(f"  Transferred {mssql_datasize - postgres_datasize} rows; "
                  f"  Time cost: {t1 - t0}, residual time: \
                  {(t1 - t0) * (mssql_datasize - postgres_datasize) / chunk_size}")
        print("   No new data acquired, finish updating Database  ---->>")

    def get_data_size(self, table: str):
        # query MSQL for amount of data

        sql_command2 = 'select count(*) from warehouse."%s"' % table
        sql_command4 = 'select count(*) from dbo."%s"' % table

        postgres_datasize = self.engine1.query(sql_command=sql_command2).iloc[0, 0]
        mssql_datasize = self.engine2.query(sql_command=sql_command4).iloc[0, 0]

        return postgres_datasize, mssql_datasize

    def update_database(self, table: str, index1: str, index2: str, data_size: int, chunk_size: int = 2000):
        # Start Query
        sql_command3 = 'select  * from "%s" order by %s ASC, %s ASC offset %s rows fetch next %d rows only' \
                       % (table, index1, index2, data_size, chunk_size)  #
        print(f'  Start transfer data , target table: {table}')
        df = self.engine2.query(sql_command=sql_command3)
        self.engine1.upload_database(df=df, table_name=table, schema=self.schema, chunk_size=chunk_size)


class GCPPostgresql:

    def __init__(self, engine="postgresql://postgres:000000Aa@220.133.246.101:55555/hdrplan"):
        from sqlalchemy import create_engine
        self.engine = create_engine(engine)
        self.connect = self.engine.connect()

    def execute(self, sql_command):
        from sqlalchemy import text
        sql_command = text(sql_command)
        self.connect.execute(sql_command)

    def query(self, sql_command):
        from sqlalchemy import text
        import pandas as pd
        sql_command = text(sql_command)
        return pd.read_sql_query(sql_command, self.connect)

    def read_sql_table(self, table_name, schema='warehouse'):
        import pandas as pd
        return pd.read_sql_table(table_name, self.connect, schema)

    def upload_database(self, df, table_name, schema: str = 'warehouse', chunk_size: int = 2000, mode='append'):
        df.to_sql(table_name, self.engine, schema=schema,
                  if_exists=mode, index=False, chunksize=chunk_size)
        print(f'  Complete Upload {table_name} to {schema}')


# 這邊可以透過許多方式來進行程式編輯，比VSCODE還要安裝許多內容真的方便許多。

class MSSQLConnect:

    def __init__(self, engine: str = "mssql+pymssql://sa:1qaz%40WSX3edc@10.120.8.231/HDRplan"):
        from sqlalchemy import create_engine
        self.engine = create_engine(engine)
        self.connect = self.engine.connect()

    def execute(self, sql_command):
        from sqlalchemy import text
        self.connect.execute(text(sql_command))

    def query(self, sql_command):
        from sqlalchemy import text
        import pandas as pd
        return pd.read_sql_query(text(sql_command), self.connect)

    def read_sql_table(self, table_name, schema='dbo'):
        import pandas as pd
        return pd.read_sql_table(table_name, self.connect, schema=schema)


postgres = GCPPostgresql()
mssql = MSSQLConnect()

table_data = {'PatientDT': ['crtno', 'birdt'], 'BPDT': ['Daidate', 'Daitime'],
              'LabDT2': ['imrapydt', 'imrregno'], 'NurDT': ['hemvsdt', 'hembed'],
              'PodDT': ['pdopdno', 'pdvsdt'], 'HDRDT': ['"Opdno"', '"Opdvsdt"'],
              'MedDT': ['pdddate', 'pddorseq'], 'predt': ['day_stamp', 'measurement_index'],
              'VaccinationsData': ['visitno', 'crtno'], 'MachineDT': ['no', 'mdtcrtno']}

if __name__ == '__main__':

    update_database = UpdateDatabase(postgres, mssql, "warehouse")

    for table in table_data:
        index1 = table_data[table][0]
        index2 = table_data[table][1]

        print(f"<<---- Table ={table}, index ={index1}, {index2}")

        update_database.update(table, index1, index2, chunk_size=100000)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
