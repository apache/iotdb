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

from udf_api.customizer.strategy.access_strategy import AccessStrategy
from udf_api.customizer.strategy.access_strategy_type import AccessStrategyType
from udf_api.exception.udf_exception import UDFException


class StateWindowAccessStrategy(AccessStrategy):
    __delta: float = 0
    __display_window_begin: int = -9223372036854775808
    __display_window_end: int = 9223372036854775807

    def __init__(
        self,
        delta: float = 0,
        display_window_begin: int = -9223372036854775808,
        display_window_end: int = 9223372036854775807,
    ):
        self.__delta = delta
        self.__display_window_begin = display_window_begin
        self.__display_window_end = display_window_end

    def check(self):
        if self.__delta < 0:
            raise UDFException(
                "Parameter delta({}) should be equal to or greater than zero.".format(
                    self.__delta
                )
            )

        if self.__display_window_begin < -9223372036854775808:
            raise UDFException(
                "Parameter display_window_begin({}) should be equal to or greater than -9223372036854775808.".format(
                    self.__display_window_begin
                )
            )

        if 9223372036854775807 < self.__display_window_end:
            raise UDFException(
                "Parameter display_window_end({}) should be equal to or less than 9223372036854775807.".format(
                    self.__display_window_end
                )
            )

        if self.__display_window_end < self.__display_window_begin:
            raise UDFException(
                "display_window_end({}) < display_window_begin({})".format(
                    self.__display_window_end, self.__display_window_begin
                )
            )

    def get_access_strategy_type(self) -> AccessStrategyType:
        return AccessStrategyType.STATE_WINDOW

    def get_delta(self) -> float:
        return self.__delta

    def get_display_window_begin(self) -> int:
        return self.__display_window_begin

    def get_display_window_end(self) -> int:
        return self.__display_window_end
