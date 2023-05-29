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
    def visit_FLOAT(self, type_, **kw):
        return "FLOAT"

    def visit_NUMERIC(self, type_, **kw):
        return "INT64"

    def visit_DECIMAL(self, type_, **kw):
        return "DOUBLE"

    def visit_INTEGER(self, type_, **kw):
        return "INT32"

    def visit_SMALLINT(self, type_, **kw):
        return "INT32"

    def visit_BIGINT(self, type_, **kw):
        return "LONG"

    def visit_TIMESTAMP(self, type_, **kw):
        return "LONG"

    def visit_text(self, type_, **kw):
        return "TEXT"
