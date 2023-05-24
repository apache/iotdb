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

from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.compiler import OPERATORS
from sqlalchemy.sql import operators


class IoTDBSQLCompiler(SQLCompiler):
    def order_by_clause(self, select, **kw):
        """allow dialects to customize how ORDER BY is rendered."""

        order_by = select._order_by_clause._compiler_dispatch(self, **kw)
        if "Time" in order_by:
            return " ORDER BY " + order_by.replace('"', "")
        else:
            return ""

    def group_by_clause(self, select, **kw):
        """allow dialects to customize how GROUP BY is rendered."""
        return ""

    def visit_select(
        self,
        select,
        asfrom=False,
        parens=True,
        fromhints=None,
        compound_index=0,
        nested_join_translation=False,
        select_wraps_for=None,
        lateral=False,
        **kwargs,
    ):
        """
        Override this method to solve two problems
        1. IoTDB does not support querying Time as a measurement name (e.g. select Time from root.storagegroup.device)
        2. IoTDB does not support path.measurement format to determine a column (e.g. select root.storagegroup.device.temperature from root.storagegroup.device)
        """
        needs_nested_translation = (
            select.use_labels
            and not nested_join_translation
            and not self.stack
            and not self.dialect.supports_right_nested_joins
        )

        if needs_nested_translation:
            transformed_select = self._transform_select_for_nested_joins(select)
            text = self.visit_select(
                transformed_select,
                asfrom=asfrom,
                parens=parens,
                fromhints=fromhints,
                compound_index=compound_index,
                nested_join_translation=True,
                **kwargs,
            )

        toplevel = not self.stack
        entry = self._default_stack_entry if toplevel else self.stack[-1]

        populate_result_map = need_column_expressions = (
            toplevel
            or entry.get("need_result_map_for_compound", False)
            or entry.get("need_result_map_for_nested", False)
        )

        if compound_index > 0:
            populate_result_map = False

        # this was first proposed as part of #3372; however, it is not
        # reached in current tests and could possibly be an assertion
        # instead.
        if not populate_result_map and "add_to_result_map" in kwargs:
            del kwargs["add_to_result_map"]

        if needs_nested_translation:
            if populate_result_map:
                self._transform_result_map_for_nested_joins(select, transformed_select)
            return text

        froms = self._setup_select_stack(select, entry, asfrom, lateral)

        column_clause_args = kwargs.copy()
        column_clause_args.update(
            {"within_label_clause": False, "within_columns_clause": False}
        )

        text = "SELECT "  # we're off to a good start !

        if select._hints:
            hint_text, byfrom = self._setup_select_hints(select)
            if hint_text:
                text += hint_text + " "
        else:
            byfrom = None

        if select._prefixes:
            text += self._generate_prefixes(select, select._prefixes, **kwargs)

        text += self.get_select_precolumns(select, **kwargs)
        # the actual list of columns to print in the SELECT column list.
        # IoTDB does not support querying Time as a measurement name (e.g. select Time from root.storagegroup.device)
        columns = []
        for name, column in select._columns_plus_names:
            column.table = None
            columns.append(
                self._label_select_column(
                    select,
                    column,
                    populate_result_map,
                    asfrom,
                    column_clause_args,
                    name=name,
                    need_column_expressions=need_column_expressions,
                )
            )
        inner_columns = [c for c in columns if c is not None]

        if populate_result_map and select_wraps_for is not None:
            # if this select is a compiler-generated wrapper,
            # rewrite the targeted columns in the result map

            translate = dict(
                zip(
                    [name for (key, name) in select._columns_plus_names],
                    [name for (key, name) in select_wraps_for._columns_plus_names],
                )
            )

            self._result_columns = [
                (key, name, tuple(translate.get(o, o) for o in obj), type_)
                for key, name, obj, type_ in self._result_columns
            ]
        # IoTDB does not allow to query Time as column,
        # need to filter out Time and pass Time and Time's alias to DBAPI separately
        # to achieve the query of Time by encoding.
        time_column_index = []
        time_column_names = []
        for i in range(len(inner_columns)):
            column_strs = (
                inner_columns[i].replace(self.preparer.initial_quote, "").split()
            )
            if "Time" in column_strs:
                time_column_index.append(str(i))
                time_column_names.append(
                    column_strs[2]
                    if OPERATORS[operators.as_] in column_strs
                    else column_strs[0]
                )
        # delete Time column
        inner_columns = list(
            filter(
                lambda x: "Time"
                not in x.replace(self.preparer.initial_quote, "").split(),
                inner_columns,
            )
        )
        if inner_columns and time_column_index:
            inner_columns[-1] = (
                inner_columns[-1]
                + " \n FROM Time Index "
                + " ".join(time_column_index)
                + "\n FROM Time Name "
                + " ".join(time_column_names)
            )

        text = self._compose_select_body(
            text, select, inner_columns, froms, byfrom, kwargs
        )

        if select._statement_hints:
            per_dialect = [
                ht
                for (dialect_name, ht) in select._statement_hints
                if dialect_name in ("*", self.dialect.name)
            ]
            if per_dialect:
                text += " " + self.get_statement_hint_text(per_dialect)

        if self.ctes and toplevel:
            text = self._render_cte_clause() + text

        if select._suffixes:
            text += " " + self._generate_prefixes(select, select._suffixes, **kwargs)

        self.stack.pop(-1)

        if (asfrom or lateral) and parens:
            return "(" + text + ")"
        else:
            return text

    def visit_table(
        self,
        table,
        asfrom=False,
        iscrud=False,
        ashint=False,
        fromhints=None,
        use_schema=True,
        **kwargs,
    ):
        """
        IoTDB's table does not support quotation marks (e.g. select ** from `root.`)
        need to override this method
        """
        if asfrom or ashint:
            effective_schema = self.preparer.schema_for_object(table)

            if use_schema and effective_schema:
                ret = effective_schema + "." + table.name
            else:
                ret = table.name
            if fromhints and table in fromhints:
                ret = self.format_from_hint_text(ret, table, fromhints[table], iscrud)
            return ret
        else:
            return ""

    def visit_column(
        self, column, add_to_result_map=None, include_table=True, **kwargs
    ):
        """
        IoTDB's where statement does not support "table".column format(e.g. "table".column > 1)
        need to override this method to return the name of column directly
        """
        return column.name
