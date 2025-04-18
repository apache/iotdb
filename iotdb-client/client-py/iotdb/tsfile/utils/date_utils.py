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

from datetime import date


class DateTimeParseException(Exception):
    pass


def parse_int_to_date(date_int: int) -> date:
    try:
        year = date_int // 10000
        month = (date_int // 100) % 100
        day = date_int % 100
        return date(year, month, day)
    except ValueError as e:
        raise DateTimeParseException("Invalid date format.") from e


def parse_date_to_int(local_date: date) -> int:
    if local_date is None:
        raise DateTimeParseException("Date expression is none or empty.")
    if local_date.year < 1000:
        raise DateTimeParseException("Year must be between 1000 and 9999.")
    return local_date.year * 10000 + local_date.month * 100 + local_date.day
