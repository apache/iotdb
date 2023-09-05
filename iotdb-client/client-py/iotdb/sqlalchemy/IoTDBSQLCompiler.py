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
        assert select_wraps_for is None, (
            "SQLAlchemy 1.4 requires use of "
            "the translate_select_structure hook for structural "
            "translations of SELECT objects"
        )

        # initial setup of SELECT.  the compile_state_factory may now
        # be creating a totally different SELECT from the one that was
        # passed in.  for ORM use this will convert from an ORM-state
        # SELECT to a regular "Core" SELECT.  other composed operations
        # such as computation of joins will be performed.

        kwargs["within_columns_clause"] = False

        compile_state = select_stmt._compile_state_factory(select_stmt, self, **kwargs)
        select_stmt = compile_state.statement

        toplevel = not self.stack

        if toplevel and not self.compile_state:
            self.compile_state = compile_state

        is_embedded_select = compound_index is not None or insert_into

        # translate step for Oracle, SQL Server which often need to
        # restructure the SELECT to allow for LIMIT/OFFSET and possibly
        # other conditions
        if self.translate_select_structure:
            new_select_stmt = self.translate_select_structure(
                select_stmt, asfrom=asfrom, **kwargs
            )

            # if SELECT was restructured, maintain a link to the originals
            # and assemble a new compile state
            if new_select_stmt is not select_stmt:
                compile_state_wraps_for = compile_state
                select_wraps_for = select_stmt
                select_stmt = new_select_stmt

                compile_state = select_stmt._compile_state_factory(
                    select_stmt, self, **kwargs
                )
                select_stmt = compile_state.statement

        entry = self._default_stack_entry if toplevel else self.stack[-1]

        populate_result_map = need_column_expressions = (
            toplevel
            or entry.get("need_result_map_for_compound", False)
            or entry.get("need_result_map_for_nested", False)
        )

        # indicates there is a CompoundSelect in play and we are not the
        # first select
        if compound_index:
            populate_result_map = False

        # this was first proposed as part of #3372; however, it is not
        # reached in current tests and could possibly be an assertion
        # instead.
        if not populate_result_map and "add_to_result_map" in kwargs:
            del kwargs["add_to_result_map"]

        froms = self._setup_select_stack(
            select_stmt, compile_state, entry, asfrom, lateral, compound_index
        )

        column_clause_args = kwargs.copy()
        column_clause_args.update(
            {"within_label_clause": False, "within_columns_clause": False}
        )

        text = "SELECT "  # we're off to a good start !

        if select_stmt._hints:
            hint_text, byfrom = self._setup_select_hints(select_stmt)
            if hint_text:
                text += hint_text + " "
        else:
            byfrom = None

        if select_stmt._independent_ctes:
            for cte in select_stmt._independent_ctes:
                cte._compiler_dispatch(self, **kwargs)

        if select_stmt._prefixes:
            text += self._generate_prefixes(
                select_stmt, select_stmt._prefixes, **kwargs
            )

        text += self.get_select_precolumns(select_stmt, **kwargs)
        # the actual list of columns to print in the SELECT column list.
        inner_columns = [
            c
            for c in [
                self._label_select_column(
                    select_stmt,
                    column,
                    populate_result_map,
                    asfrom,
                    column_clause_args,
                    name=name,
                    proxy_name=proxy_name,
                    fallback_label_name=fallback_label_name,
                    column_is_repeated=repeated,
                    need_column_expressions=need_column_expressions,
                )
                for (
                    name,
                    proxy_name,
                    fallback_label_name,
                    column,
                    repeated,
                ) in compile_state.columns_plus_names
            ]
            if c is not None
        ]

        if populate_result_map and select_wraps_for is not None:
            # if this select was generated from translate_select,
            # rewrite the targeted columns in the result map

            translate = dict(
                zip(
                    [
                        name
                        for (
                            key,
                            proxy_name,
                            fallback_label_name,
                            name,
                            repeated,
                        ) in compile_state.columns_plus_names
                    ],
                    [
                        name
                        for (
                            key,
                            proxy_name,
                            fallback_label_name,
                            name,
                            repeated,
                        ) in compile_state_wraps_for.columns_plus_names
                    ],
                )
            )

            self._result_columns = [
                (key, name, tuple(translate.get(o, o) for o in obj), type_)
                for key, name, obj, type_ in self._result_columns
            ]

        # change the superset aggregate function name into iotdb aggregate function name
        # by matching the head of aggregate function name and replace it.
        for i in range(len(inner_columns)):
            if inner_columns[i].startswith("max("):
                inner_columns[i] = inner_columns[i].replace("max(", "max_value(")
            if inner_columns[i].startswith("min("):
                inner_columns[i] = inner_columns[i].replace("min(", "min_value(")
            if inner_columns[i].startswith("count(DISTINCT"):
                inner_columns[i] = inner_columns[i].replace("count(DISTINCT", "count(")

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
                + " \n FROM Time Name "
                + " ".join(time_column_names)
            )

        text = self._compose_select_body(
            text,
            select_stmt,
            compile_state,
            inner_columns,
            froms,
            byfrom,
            toplevel,
            kwargs,
        )

        if select_stmt._statement_hints:
            per_dialect = [
                ht
                for (dialect_name, ht) in select_stmt._statement_hints
                if dialect_name in ("*", self.dialect.name)
            ]
            if per_dialect:
                text += " " + self.get_statement_hint_text(per_dialect)

        # In compound query, CTEs are shared at the compound level
        if self.ctes and (not is_embedded_select or toplevel):
            nesting_level = len(self.stack) if not toplevel else None
            text = (
                self._render_cte_clause(
                    nesting_level=nesting_level,
                    visiting_cte=kwargs.get("visiting_cte"),
                )
                + text
            )

        if select_stmt._suffixes:
            text += " " + self._generate_prefixes(
                select_stmt, select_stmt._suffixes, **kwargs
            )

        self.stack.pop(-1)
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
