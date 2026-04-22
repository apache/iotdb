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

from sqlalchemy.sql.compiler import DDLCompiler


class IoTDBDDLCompiler(DDLCompiler):
    def visit_create_column(self, create, first_pk=False, **kw):
        column = create.element

        if column.system:
            return None

        category = column.dialect_options["iotdb"].get("category")

        if category and category.upper() == "TIME":
            colspec = self.preparer.format_column(column) + " TIME"
            return colspec

        colspec = (
            self.preparer.format_column(column)
            + " "
            + self.dialect.type_compiler_instance.process(
                column.type, type_expression=column
            )
        )

        if category:
            colspec += " " + category.upper()

        return colspec

    def post_create_table(self, table):
        ttl = table.dialect_options["iotdb"].get("ttl")
        if ttl is not None:
            return " WITH (TTL=%d)" % int(ttl)
        return ""

    def create_table_constraints(self, table, **kw):
        return ""

    def visit_primary_key_constraint(self, constraint, **kw):
        return None

    def visit_foreign_key_constraint(self, constraint, **kw):
        return None

    def visit_unique_constraint(self, constraint, **kw):
        return None

    def visit_check_constraint(self, constraint, **kw):
        return None
