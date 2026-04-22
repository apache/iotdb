# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from sqlalchemy import schema as sa_schema, types
from sqlalchemy.engine import default
from sqlalchemy.sql import text

from iotdb import dbapi

from .IoTDBDDLCompiler import IoTDBDDLCompiler
from .IoTDBIdentifierPreparer import IoTDBIdentifierPreparer
from .IoTDBSQLCompiler import IoTDBSQLCompiler
from .IoTDBTypeCompiler import IoTDBTypeCompiler

IOTDB_CATEGORY_TIME = "TIME"
IOTDB_CATEGORY_TAG = "TAG"
IOTDB_CATEGORY_ATTRIBUTE = "ATTRIBUTE"
IOTDB_CATEGORY_FIELD = "FIELD"

ischema_names = {
    "BOOLEAN": types.Boolean,
    "INT32": types.Integer,
    "INT64": types.BigInteger,
    "FLOAT": types.Float,
    "DOUBLE": types.Float,
    "STRING": types.String,
    "TEXT": types.Text,
    "BLOB": types.LargeBinary,
    "TIMESTAMP": types.DateTime,
    "DATE": types.Date,
}


class IoTDBDialect(default.DefaultDialect):
    name = "iotdb"
    driver = "iotdb"

    statement_compiler = IoTDBSQLCompiler
    ddl_compiler = IoTDBDDLCompiler
    type_compiler_cls = IoTDBTypeCompiler
    preparer = IoTDBIdentifierPreparer

    supports_alter = True
    supports_schemas = True
    supports_sequences = False
    supports_native_boolean = True
    supports_native_enum = False
    supports_statement_cache = True
    insert_returning = False
    update_returning = False
    delete_returning = False
    supports_default_values = False
    supports_empty_insert = False
    postfetch_lastrowid = False
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    construct_arguments = [
        (sa_schema.Column, {"category": None}),
        (sa_schema.Table, {"ttl": None}),
    ]

    @classmethod
    def import_dbapi(cls):
        return dbapi

    @classmethod
    def dbapi(cls):
        return dbapi

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        opts.update(url.query)
        opts["sql_dialect"] = "table"
        return ([], opts)

    def initialize(self, connection):
        pass

    def _get_server_version_info(self, connection):
        return None

    def _get_default_schema_name(self, connection):
        return None

    def has_schema(self, connection, schema_name, **kw):
        return schema_name in self.get_schema_names(connection)

    def has_table(self, connection, table_name, schema=None, **kw):
        return table_name in self.get_table_names(connection, schema=schema)

    def get_schema_names(self, connection, **kw):
        cursor = connection.execute(text("SHOW DATABASES"))
        return [row[0] for row in cursor.fetchall()]

    def get_table_names(self, connection, schema=None, **kw):
        if schema:
            connection.execute(text("USE %s" % schema))
        cursor = connection.execute(text("SHOW TABLES"))
        return [row[0] for row in cursor.fetchall()]

    def get_columns(self, connection, table_name, schema=None, **kw):
        if schema:
            connection.execute(text("USE %s" % schema))
        cursor = connection.execute(
            text("SHOW COLUMNS FROM %s" % table_name)
        )
        columns = []
        for row in cursor.fetchall():
            col_name = row[0]
            col_type_str = row[1]
            col_category = row[2] if len(row) > 2 else None

            sa_type = ischema_names.get(col_type_str.upper(), types.UserDefinedType)

            col_info = {
                "name": col_name,
                "type": sa_type() if isinstance(sa_type, type) else sa_type,
                "nullable": True,
                "default": None,
            }

            if col_category:
                col_info["iotdb_category"] = col_category.upper()

            columns.append(col_info)

        return columns

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def do_commit(self, dbapi_connection):
        pass

    def do_rollback(self, dbapi_connection):
        pass
