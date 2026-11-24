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

import time
from datetime import date, datetime, time as datetime_time

from iotdb.utils.IoTDBConstants import TSDataType


class DBAPITypeObject(object):
    def __init__(self, *values):
        self.values = frozenset(values)

    def __eq__(self, other):
        if isinstance(other, DBAPITypeObject):
            return self is other
        try:
            return other in self.values
        except TypeError:
            return False

    def __ne__(self, other):
        return not self == other


STRING = DBAPITypeObject(TSDataType.TEXT, TSDataType.STRING)
BINARY = DBAPITypeObject(TSDataType.BLOB)
NUMBER = DBAPITypeObject(
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
)
DATETIME = DBAPITypeObject(TSDataType.DATE, TSDataType.TIMESTAMP)
ROWID = DBAPITypeObject()


def Date(year, month, day):
    return date(year, month, day)


def Time(hour, minute, second):
    return datetime_time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    return datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


def Binary(value):
    return bytes(value)
