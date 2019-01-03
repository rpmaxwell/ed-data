from datetime import datetime
import json
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, insert, create_engine
from sqlalchemy.dialects import postgresql
from io import StringIO
import csv

class Conn(object):
    def __init__(self, environment=None, schema_name=None):
        """Create a shell to hold metadata."""
        self.schema_name = schema_name
        cred_file_path = os.environ['CRED_FILE_LOCATION']
        cred_payload = json.loads(open(cred_file_path).read())[environment]
        url = 'postgresql://{user}:{password}@{host}/{db}'.format(**cred_payload)
        self.engine = create_engine(url)
        self.metadata = MetaData(schema=schema_name)
        self.metadata.bind = self.engine

    @staticmethod
    def get_insert_statement(table, params, **upsert_kwargs):
        stmt = postgresql.insert(table).values(params)
        if upsert_kwargs:
            upsert_statement = stmt.on_conflict_do_update(**upsert_kwargs)
        else:
            upsert_statement = stmt
        return upsert_statement

    def upsert(self, table_name, data, **upsert_kwargs):
        table = Table(table_name, self.metadata, schema=self.schema_name, autoload=True)
        insert_statement = self.get_insert_statement(table, data, **upsert_kwargs)
        with self.engine.connect() as conn:
            result = conn.execute(insert_statement)
        return result

    def execute_raw_query(self, query, fmt='df', params=None):
        """Take a SQL query in string format and then executes in Presto."""
        params = params or {}
        proxy = self.engine.execute(sqlalchemy.text(query), params)
        try:
            results_obj = self._format_query(proxy, fmt)
            return results_obj
        except:
            return None
            
    @staticmethod
    def _format_query(proxy, fmt):
        '''
        returns the results of the proxy object in the specified format (dataframe, dict, list etc.)
        Pandas Dataframes are handled explictly while others are just applied using the eval
        function.
        TODO:
        add timezone and datatype considerations as in dconn.
        '''
        if fmt == 'df':
            columns = [str(k) for k in proxy.keys()]
            results_obj = pd.DataFrame(proxy.fetchall(), columns=columns)
        else:
            results_obj = list(map(eval(fmt), proxy))
        return results_obj

    def create_table_from_columns(self, table_name, column_dtype_dict):
        dtype_map = {'int64': sqlalchemy.INTEGER,
                     'object': sqlalchemy.VARCHAR,
                     'float64': sqlalchemy.FLOAT,
                     'datetime64[ns]': sqlalchemy.TIMESTAMP
                    }
        column_dtype_dict = {k: dtype_map[v.name] for k, v in column_dtype_dict.items()}
        table = Table(table_name, self.metadata,
                 *(Column(column_name, column_type)
                   for column_name,
                       column_type in column_dtype_dict.items()))

        table.create(bind=self.engine)

    def copy_to_db(self, table_name, df, conflict_str, sep='\t'):
        """
        1) convert the dataframe to a string object
        2) write to a temp table
        3) diff the temp table in sql
        4) upsert the changes to analytics_prototypes.merchant_forecasts
        """
        if not self.engine.dialect.has_table(self.engine, table_name, schema=self.schema_name):
            self.create_table_from_columns(table_name, dict(zip(df.columns, df.dtypes)))
            create_constraint = '''
            ALTER TABLE {}.{} ADD UNIQUE {};
            '''.format(self.schema_name, table_name, conflict_str)
            self.execute_raw_query(create_constraint)
        string_buffer = StringIO()
        df.to_csv(string_buffer, header=False,
                  index=False, encoding='utf-8', sep=sep)
        string_buffer.seek(0)
        raw_conn = self.engine.raw_connection()
        query_params = {
            'table_name': table_name,
            'temp_table_name': 'temp_{}'.format(table_name),
            'schema': self.schema_name
        }
        with raw_conn.cursor() as cursor:
            cursor.execute(
                '''
                CREATE TEMP TABLE
                                {temp_table_name}
                (LIKE 
                                {schema}.{table_name}
                                INCLUDING INDEXES);
                '''.format(**query_params)
            )
            cursor.copy_from(string_buffer, query_params['temp_table_name'], sep=sep, null="",
                                            columns=tuple(df.columns.values))
            cursor.execute(
                '''
                INSERT INTO {schema}.{table_name}
                    SELECT * FROM {temp_table_name}
                ON CONFLICT {conflict} DO NOTHING
                '''.format(**query_params, conflict=conflict_str)
                )
            cursor.execute("""DROP TABLE {};""".format(query_params['temp_table_name']))
        raw_conn.commit()
        raw_conn.close()
        string_buffer.close()






