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

import math
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from numbers import Integral, Real

from .Exceptions import DataError, ProgrammingError

# Match only the advertised string placeholders and percent escapes. A bare
# percent catches unsupported conversions, widths and precisions before they
# can modify an already escaped SQL value. This is not a SQL lexer.
_PLACEHOLDER = re.compile(r"%(?:%|s|\([^()]+\)s)?")


def format_operation(operation, parameters):
    if not isinstance(operation, str):
        raise ProgrammingError("operation must be a string")
    if parameters is None:
        return operation

    named = isinstance(parameters, Mapping)
    if not named and (
        isinstance(parameters, (str, bytes, bytearray, memoryview))
        or not isinstance(parameters, Sequence)
    ):
        raise ProgrammingError("parameters must be a sequence or mapping")

    for match in _PLACEHOLDER.finditer(operation):
        token = match.group()
        if token == "%%":
            continue
        if token == "%" or named != token.startswith("%("):
            raise ProgrammingError(
                "use %(name)s with a mapping or %s with a sequence; "
                "escape literal percent signs as %%"
            )

    if not named and not parameters:
        has_value_placeholder = any(
            match.group() != "%%" for match in _PLACEHOLDER.finditer(operation)
        )
        if not has_value_placeholder:
            return operation % () if "%%" in operation else operation

    try:
        values = (
            _LiteralMapping(parameters)
            if named
            else tuple(_sql_literal(value) for value in parameters)
        )
        return operation % values
    except (KeyError, TypeError, ValueError) as error:
        raise ProgrammingError("SQL parameters do not match placeholders") from error


class _LiteralMapping:
    """Convert only referenced values; unused mapping entries are allowed."""

    def __init__(self, parameters):
        self.parameters = parameters

    def __getitem__(self, name):
        return _sql_literal(self.parameters[name])


def _sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, Integral):
        return str(int(value))
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise DataError("non-finite decimal values are not supported")
        return str(value)
    if isinstance(value, Real):
        converted = float(value)
        if not math.isfinite(converted):
            raise DataError("non-finite floating-point values are not supported")
        return repr(converted)
    if isinstance(value, datetime):
        return _quote(value.isoformat(sep=" "))
    if isinstance(value, date):
        return _quote(value.isoformat())
    if isinstance(value, time):
        return _quote(value.isoformat())
    if isinstance(value, str):
        return _quote(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "X'{}'".format(bytes(value).hex().upper())
    raise DataError("unsupported parameter type: {}".format(type(value).__name__))


def _quote(value):
    return "'{}'".format(value.replace("'", "''"))
