import argparse
import random
import time
import sys
import json
import pandas as pd
from sqlalchemy import *
from sqlalchemy.engine import Engine
from pymongo import *
from pymongo.errors import ConnectionFailure
from urllib.parse import urlparse

"""SQLAlchemy params
"""
engine = None
metadata_obj = None


def giveme_engine(uri):
    global engine
    global metadata_obj
    url = urlparse(uri)
    if "mongodb" == url.scheme:
        engine = MongoClient(uri)
        try:
            # The ping command is cheap and does not require auth.
            engine.admin.command('ping')
            metadata_obj = {"db": engine[url.path.replace("/", "")]}
        except ConnectionFailure as e:
            print("Mongo server not available: {}".format(str(e)))
            exit(1)
    else:
        engine = create_engine(uri)
        metadata_obj = MetaData()


def get_random_string(length):
    """Generate a random string with `lenght` chars, with this charset: 'abcdefghijklmnopqrstuvwxyz0123456789'.

        Params:
        - lenght (integer): Indicates the lenght of string. It must be greather than 0.

        Returns:
        - string
    """
    if length < 1:
        raise ValueError("Random string lenght must be greater than 0")
    return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for i in range(length))


def get_random_datatype():
    """Returns a random datatype name.

        Params: None

        Returns:
        - string
    """
    return random.choice(["Boolean", "Integer", "Float", "String(100)"])


def get_table_from_metadata(table_name):
    """Get table object from SQLAlchemy metadata.

        Params:
        - table_name (string): Table name to get.

        Returns:
        - SQLAlchemy Table
    """
    for t in metadata_obj.sorted_tables:
        if t.name == table_name:
            return t


def get_columns_from_metadata(table, include_key=False):
    """Get columns objects from SQLAlchemy metadata.

        Params:
        - table (SQLAlchemy Table): Table to get columns.
        - include_key (boolean): If True then includes column key `col_key`.

        Returns:
        - List of SQLAlchemy Column
    """
    columns = []
    for c in table.c:
        if ("col_key" in str(c)):
            if include_key:
                columns.append(c)
        else:
            columns.append(c)
    return columns


def generate_schema(tables, cols):
    """Returns a schema in JSON.

        Params:
        - tables (integer): Indicates the table number to generate. It must be greather than 0.
        - cols (integer): Indicates the columns number to generate in each table. It must be greather than 0.

        Returns:
        - string
    """
    if tables < 1:
        raise ValueError("Tables number must be greater than 0")
    if cols < 1:
        raise ValueError("Columns number must be greater than 0")
    schema = {}
    for _ in range(tables):
        tablename = "{}_{}".format("table", get_random_string(6))
        columns_schema = {"col_key": "Integer"}
        for i in range(cols):
            columns_schema["{}_{}".format("col", get_random_string(6))] = get_random_datatype()
        schema[tablename] = columns_schema
    return schema


def create_tables(schema_json):
    """Create tables based on `schema_json` parameter.

        Params:
        - schema_json (dict): Dict with table definition. Must be generated with `schema_generator.py`.

        Returns: None
    """
    if isinstance(engine, Engine):
        for table in schema_json.keys():
            create_stmt = []
            create_stmt.append("Table('{}', metadata_obj,".format(table))
            cols = []
            for col,typ in schema_json[table].items():
                if col == "col_key":
                    cols.append("Column('{}', {}, Identity(start=1), primary_key=True, autoincrement=True)".format(col, typ))
                else:
                    cols.append("Column('{}', {})".format(col, typ))
            create_stmt.append(", ".join(cols))
            create_stmt.append(")")
            create_stmt = " ".join(create_stmt)
            exec(create_stmt)
        metadata_obj.create_all(engine)
    elif isinstance(engine, MongoClient):
        pass


def drop_tables(schema_json):
    """Drop tables based on `schema_json` parameter.

        Params:
        - schema_json (dict): Dict with table definition. Must be generated with `schema_generator.py`.

        Returns: None
    """
    if isinstance(engine, Engine):
        drop_tables = []
        for table in schema_json.keys():
            # get table from metadata
            drop_tables.append(get_table_from_metadata(table))
        metadata_obj.drop_all(engine, tables=drop_tables)
    elif isinstance(engine, MongoClient):
        db = metadata_obj["db"]
        for table in schema_json.keys():
            collection = db[table]
            collection.drop()


def generate_value(type):
    """Generate value based on `type` parameter.

        Params:
        - type (string): Data type to generate. Must be one of: \"Boolean\", \"Integer\", \"Float\", \"String\"

        Returns:
        - Any of boolean, int, float or string.
    """
    if type == "Boolean":
        return random.choice([True, False])
    elif type == "Integer":
        return random.randint(0, 2000000000)
    elif type == "Float":
        return random.uniform(0.0, 2000000000.0)
    elif type == "String" or type == "String(100)":
        return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789 ') for i in range(random.randint(10, 100)))


def insert_records(table, columns_schema, records):
    """Generate new dataframe based on `columns_schema` parameter and insert into db.

        Params:
        - table (string): Table name to update.
        - columns_schema (dict): Dict with table columns definition.
        - records (int): Number of records to be generated.

        Returns:
        - dict
    """
    if isinstance(engine, Engine):
        # create dataframe data
        df_data = {}
        del columns_schema["col_key"]
        for i in range(records):
            row = []
            for datatype in columns_schema.values():
                row.append(generate_value(datatype))
            df_data[i] = row
        # insert dataframe into db
        df = pd.DataFrame.from_dict(df_data, columns=columns_schema.keys(), orient="index")
        df.to_sql(table, con=engine, if_exists="append", index=False, index_label="col_key")
    elif isinstance(engine, MongoClient):
        db = metadata_obj["db"]
        collection = db[table]
        # create dataframe data
        df_data = []
        del columns_schema["col_key"]
        for i in range(records):
            document = {}
            for column_name, datatype in columns_schema.items():
                document[column_name] = generate_value(datatype)
            df_data.append(document)
        # insert dataframe into db
        collection.insert_many(df_data)


def update_records(table, columns_schema, records):
    """Random update dataframe data based on `columns_schema` parameter and update records into db.

        Params:
        - table (string): Table name to update.
        - columns_schema (dict): Dict with table columns definition.
        - records (int): Number of records to be generated. If it is greater than dataframe length, then it is set to dataframe length.

        Returns:
        - dict
    """
    if isinstance(engine, Engine):
        # get rows from db to modify
        df = pd.read_sql("""SELECT {} FROM {}""".format("col_key", table), engine)
        if records > len(df):
            records = len(df)
        df = df.sample(n=records)
        #print(df)
        # generate new values and update on tables
        table_obj = get_table_from_metadata(table)
        new_df_data = {}
        del columns_schema["col_key"]
        conn = engine.connect()
        trans = conn.begin()
        try:
            for _, rows in df.iterrows():
                for _, col_key in rows.items():
                    values = {}
                    for column_name, datatype in columns_schema.items():
                        values[column_name] = generate_value(datatype)
                    stmt = update(table_obj).where(table_obj.c.col_key==col_key).values(values)
                    engine.execute(stmt)
            trans.commit()
        except:
            trans.rollback()
            raise
    elif isinstance(engine, MongoClient):
        db = metadata_obj["db"]
        collection = db[table]
        documents = collection.aggregate([{ "$sample": { "size": records } }])
        del columns_schema["col_key"]
        for document in documents:
            new_document = {}
            for column_name, datatype in columns_schema.items():
                document[column_name] = generate_value(datatype)
            collection.update_one({ "_id": document["_id"] }, { "$set": new_document })


def delete_records(table, records):
    """Delete random records.

        Params:
        - table (string): Table name to update.
        - records (int): Number of records to be generated. If it is greater than dataframe length, then it is set to dataframe length.

        Returns: None
    """
    if isinstance(engine, Engine):
        # get table from metadata
        table_obj = get_table_from_metadata(table)
        df = pd.read_sql("""SELECT {} FROM {}""".format("col_key", table), engine)
        conn = engine.connect()
        trans = conn.begin()
        try:
            if records >= len(df):
                engine.execute(delete(table_obj))
            else:
                df = df.sample(n=records)
                #print(df)
                for index, _ in df.iterrows():
                    engine.execute(delete(table_obj).where(table_obj.c.col_key==index))
            trans.commit()
        except:
            trans.rollback()
            raise
    elif isinstance(engine, MongoClient):
        db = metadata_obj["db"]
        collection = db[table]
        documents = collection.aggregate([{ "$sample": { "size": records } }])
        for document in documents:
            collection.delete_one({ "_id": document["_id"] })


def argparse_menu(parser):
    subparsers_main = parser.add_subparsers(help='Subcommands', dest="command")
    # generate schema
    ## genera un esquema basado en num de tablas y columnas y lo puede sacar por pantalla o en archivo de texto
    parser_schema = subparsers_main.add_parser('schema', help='Generate schema')
    parser_schema.add_argument('--tables', '-t', required=True, type=int, action='store', help='Number of tables to generate in schema')
    parser_schema.add_argument('--columns', '-c', required=True, type=int, action='store', help='Number of columns to generate in each table of generated schema')
    parser_schema.add_argument('--outfile', '-o', nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='Save in JSON file')
    # create
    ## crea en bd un esquema pasado por archivo o stdin, ademas necesita la cadena de conexion
    parser_create = subparsers_main.add_parser('create', help='Create schema tables')
    parser_create.add_argument('--database', '-d', required=True, action='store', help='Database connection chain')
    parser_create.add_argument('--schema', '-s', nargs='?', type=argparse.FileType('r'), default=sys.stdin, help='Read schema from JSON file. By default is stdin')
    # drop
    ## elimina de la bd las tablas del esquema pasado por archivo o stdin, ademas necesita la cadena de conexion
    parser_create = subparsers_main.add_parser('drop', help='Drop schema tables')
    parser_create.add_argument('--database', '-d', required=True, action='store', help='Database connection chain')
    parser_create.add_argument('--schema', '-s', nargs='?', type=argparse.FileType('r'), default=sys.stdin, help='Read schema from JSON file. By default is stdin')
    # insert
    ## inserta en bd datos aleatorios, necesita el esquema por archivo o stdin, ademas necesita la cadena de conexion y el num de registros a insertar
    parser_insert = subparsers_main.add_parser('insert', help='Insert random records on tables')
    parser_insert.add_argument('--database', '-d', required=True, action='store', help='Database connection chain')
    parser_insert.add_argument('--schema', '-s', nargs='?', type=argparse.FileType('r'), default=sys.stdin, help='Read schema from JSON file. By default is stdin')
    parser_insert.add_argument('--records', '-r', required=True, type=int, action='store', help='Number of records to insert on each table')
    # update
    ## actualiza en bd datos aleatorios, necesita el esquema por archivo o stdin, ademas necesita la cadena de conexion y el num de registros a modificar
    parser_insert = subparsers_main.add_parser('update', help='Update random records on tables')
    parser_insert.add_argument('--database', '-d', required=True, action='store', help='Database connection chain')
    parser_insert.add_argument('--schema', '-s', nargs='?', type=argparse.FileType('r'), default=sys.stdin, help='Read schema from JSON file. By default is stdin')
    parser_insert.add_argument('--records', '-r', required=True, type=int, action='store', help='Number of records to update on each table')
    # delete
    ## elimina en bd filas aleatorias, necesita el esquema por archivo o stdin, ademas necesita la cadena de conexion y el num de registros a modificar
    parser_insert = subparsers_main.add_parser('delete', help='Delete random records on tables')
    parser_insert.add_argument('--database', '-d', required=True, action='store', help='Database connection chain')
    parser_insert.add_argument('--schema', '-s', nargs='?', type=argparse.FileType('r'), default=sys.stdin, help='Read schema from JSON file. By default is stdin')
    parser_insert.add_argument('--records', '-r', required=True, type=int, action='store', help='Number of records to delete on each table')
    # random
    ## genera un esquema, lo crea en bd e inserta datos aleatorios, necesita la cadena de conexion y el num de registros a insertar
    parser_random = subparsers_main.add_parser('random', help='Generate schema, create it and insert random records')
    parser_random.add_argument('--database', '-d', required=True, action='store', help='Database connection chain')
    parser_random.add_argument('--tables', '-t', required=True, type=int, action='store', help='Number of tables to generate in schema')
    parser_random.add_argument('--columns', '-c', required=True, type=int, action='store', help='Number of columns to generate in each table of generated schema')
    parser_random.add_argument('--records', '-r', required=True, type=int, action='store', help='Number of records to insert on each table')
    return parser.parse_args()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CRUD Stress Tool - github.com/aramcap')
    args = argparse_menu(parser)
    start_time = time.time()
    
    if args.command == "schema":
        # check arguments
        if args.tables == None or args.columns == None or args.outfile == None:
            parser.print_help()
            sys.exit(2)
        # generate schema and convert to json
        schema = json.dumps(generate_schema(int(args.tables), int(args.columns)))
        # write out schema
        args.outfile.write(schema)
    elif args.command == "create":
        # check arguments
        if args.database == None or args.schema == None:
            parser.print_help()
            sys.exit(2)
        # extract schema from input and convert to json
        for line in args.schema:
            schema = json.loads(line)
        # create db engine from database arg
        giveme_engine(args.database)
        # create tables
        create_tables(schema)
        print("--- Create time: {} secs ---".format(time.time() - start_time))
    elif args.command == "drop":
        # check arguments
        if args.database == None or args.schema == None:
            parser.print_help()
            sys.exit(2)
        # extract schema from input and convert to json
        for line in args.schema:
            schema = json.loads(line)
        # create db engine from database arg
        giveme_engine(args.database)
        # create tables
        create_tables(schema)
        # drop tables
        drop_tables(schema)
        print("--- Drop time: {} secs ---".format(time.time() - start_time))
    elif args.command == "insert":
        # check arguments
        if args.database == None or args.schema == None or args.records == None:
            parser.print_help()
            sys.exit(2)
        # extract schema from input and convert to json
        for line in args.schema:
            schema = json.loads(line)
        # create db engine from database arg
        giveme_engine(args.database)
        # generate dataframe and insert into db
        for table,cols in schema.items():
            start_insert_time = time.time()
            insert_records(table, cols, args.records)
            print("--- Insert per table time: {} secs ---".format(time.time() - start_insert_time))
    elif args.command == "update":
        # check arguments
        if args.database == None or args.schema == None or args.records == None:
            parser.print_help()
            sys.exit(2)
        # extract schema from input and convert to json
        for line in args.schema:
            schema = json.loads(line)
        # create db engine from database arg
        giveme_engine(args.database)
        # create tables
        create_tables(schema)
        # generate dataframe and insert into db
        for table,cols in schema.items():
            start_update_time = time.time()
            update_records(table, cols, args.records)
            print("--- Update per table time: {} secs ---".format(time.time() - start_update_time))
    elif args.command == "delete":
        # check arguments
        if args.database == None or args.schema == None or args.records == None:
            parser.print_help()
            sys.exit(2)
        # extract schema from input and convert to json
        for line in args.schema:
            schema = json.loads(line)
        # create db engine from database arg
        giveme_engine(args.database)
        # create tables
        create_tables(schema)
        # generate dataframe and insert into db
        for table,cols in schema.items():
            start_delete_time = time.time()
            delete_records(table, args.records)
            print("--- Delete per table time: {} secs ---".format(time.time() - start_delete_time))
    elif args.command == "random":
        # check arguments
        if args.database == None or args.tables == None or args.columns == None or args.records == None:
            parser.print_help()
            sys.exit(2)
        # create db engine from database arg
        giveme_engine(args.database)
        # generate schema 
        schema = generate_schema(int(args.tables), int(args.columns))
        start_create_time = time.time()
        # create tables
        create_tables(schema)
        print("--- Create time: {} secs ---".format(time.time() - start_create_time))
        # generate dataframe and insert into db
        for table,cols in schema.items():
            start_insert_time = time.time()
            insert_records(table, cols, args.records)
            print("--- Insert per table time: {} secs ---".format(time.time() - start_insert_time))
    else:
        parser.print_help()
        sys.exit(2)
    
    print("--- Execution time: {} secs ---".format(time.time() - start_time))
