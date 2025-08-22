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

from sqlalchemy import types, util
from sqlalchemy.engine import default
from sqlalchemy.sql import text
from sqlalchemy.sql.sqltypes import String

from iotdb import dbapi

from .IoTDBIdentifierPreparer import IoTDBIdentifierPreparer
from .IoTDBSQLCompiler import IoTDBSQLCompiler
from .IoTDBTypeCompiler import IoTDBTypeCompiler

TYPES_MAP = {
    "BOOLEAN": types.Boolean,
    "INT32": types.Integer,
    "INT64": types.BigInteger,
    "FLOAT": types.Float,
    "DOUBLE": types.Float,
    "TEXT": types.Text,
    "LONG": types.BigInteger,
}


class IoTDBDialect(default.DefaultDialect):
    name = "iotdb"
    driver = "iotdb-python"
    statement_compiler = IoTDBSQLCompiler
    type_compiler = IoTDBTypeCompiler
    preparer = IoTDBIdentifierPreparer
    convert_unicode = True

    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_simple_order_by_label = False
    supports_schemas = True
    supports_right_nested_joins = False
    description_encoding = None

    if hasattr(String, "RETURNS_UNICODE"):
        returns_unicode_strings = String.RETURNS_UNICODE
    else:

        def _check_unicode_returns(self, connection, additional_tests=None):
            return True

        _check_unicode_returns = _check_unicode_returns

    def create_connect_args(self, url):
        # inherits the docstring from interfaces.Dialect.create_connect_args
        opts = url.translate_connect_args()
        opts.update(url.query)
        opts.update({"sqlalchemy_mode": True})
        return [[], opts]

    @classmethod
    def import_dbapi(cls):
        return dbapi

    @classmethod
    def dbapi(cls):
        return dbapi

    def has_schema(self, connection, schema):
        return schema in self.get_schema_names(connection)

    def has_table(self, connection, table_name, schema=None, **kw):
        return table_name in self.get_table_names(connection, schema=schema)

    def get_schema_names(self, connection, **kw):
        cursor = connection.execute(text("SHOW DATABASES"))
        return [row[0] for row in cursor.fetchall()]

    def get_table_names(self, connection, schema=None, **kw):
        cursor = connection.execute(
            text("SHOW DEVICES %s.**" % (schema or self.default_schema_name))
        )
        return [row[0].replace(schema + ".", "", 1) for row in cursor.fetchall()]

    def get_columns(self, connection, table_name, schema=None, **kw):
        cursor = connection.execute(
            text("SHOW TIMESERIES %s.%s.*" % (schema, table_name))
        )
        columns = [self._general_time_column_info()]
        for row in cursor.fetchall():
            columns.append(self._create_column_info(row, schema, table_name))
        return columns

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        pass

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    @util.memoized_property
    def _dialect_specific_select_one(self):
        # IoTDB does not support select 1
        # so replace the statement with "show version"
        return "SHOW VERSION"

    def _general_time_column_info(self):
        """
        Treat Time as a column
        """
        return {
            "name": "Time",
            "type": self._resolve_type("LONG"),
            "nullable": False,
            "default": None,
        }

    def _create_column_info(self, row, schema, table_name):
        """
        Generate description information for each column
        """
        return {
            "name": row[0].replace(schema + "." + table_name + ".", "", 1),
            "type": self._resolve_type(row[3]),
            "nullable": True,
            "default": None,
        }

    def _resolve_type(self, type_):
        return TYPES_MAP.get(type_, types.UserDefinedType)
