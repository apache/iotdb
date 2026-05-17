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

from sqlalchemy.sql.compiler import GenericTypeCompiler


class IoTDBTypeCompiler(GenericTypeCompiler):
    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_FLOAT(self, type_, **kw):
        return "FLOAT"

    def visit_REAL(self, type_, **kw):
        return "FLOAT"

    def visit_NUMERIC(self, type_, **kw):
        return "DOUBLE"

    def visit_DECIMAL(self, type_, **kw):
        return "DOUBLE"

    def visit_INTEGER(self, type_, **kw):
        return "INT32"

    def visit_SMALLINT(self, type_, **kw):
        return "INT32"

    def visit_BIGINT(self, type_, **kw):
        return "INT64"

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"

    def visit_DATETIME(self, type_, **kw):
        return "TIMESTAMP"

    def visit_DATE(self, type_, **kw):
        return "DATE"

    def visit_TEXT(self, type_, **kw):
        return "STRING"

    def visit_VARCHAR(self, type_, **kw):
        return "STRING"

    def visit_NVARCHAR(self, type_, **kw):
        return "STRING"

    def visit_CHAR(self, type_, **kw):
        return "STRING"

    def visit_BLOB(self, type_, **kw):
        return "BLOB"

    def visit_BINARY(self, type_, **kw):
        return "BLOB"

    def visit_VARBINARY(self, type_, **kw):
        return "BLOB"

    def visit_LARGE_BINARY(self, type_, **kw):
        return "BLOB"

    def visit_large_binary(self, type_, **kw):
        return "BLOB"

    def visit_boolean(self, type_, **kw):
        return "BOOLEAN"

    def visit_string(self, type_, **kw):
        return "STRING"

    def visit_unicode(self, type_, **kw):
        return "STRING"

    def visit_text(self, type_, **kw):
        return "STRING"

    def visit_unicode_text(self, type_, **kw):
        return "STRING"

    def visit_float(self, type_, **kw):
        return "FLOAT"

    def visit_numeric(self, type_, **kw):
        return "DOUBLE"

    def visit_integer(self, type_, **kw):
        return "INT32"

    def visit_big_integer(self, type_, **kw):
        return "INT64"

    def visit_timestamp(self, type_, **kw):
        return "TIMESTAMP"

    def visit_datetime(self, type_, **kw):
        return "TIMESTAMP"

    def visit_date(self, type_, **kw):
        return "DATE"
