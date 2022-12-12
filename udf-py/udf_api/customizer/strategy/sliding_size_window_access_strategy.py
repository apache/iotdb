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


class SlidingSizeWindowAccessStrategy(AccessStrategy):
    """
    Used in UDTF#beforeStart(UDFParameters, UDTFConfigurations).

    When the access strategy of a UDTF is set to an instance of this class, the method UDTF#transform(RowWindow,
    PointCollector) of the UDTF will be called to transform the original data. You need to override the method in your
    own UDTF class.

    Sliding size window is a kind of size-based window. Except for the last call, each call of the method
    UDTF#transform(RowWindow, PointCollector) processes a window with windowSize rows (aligned by time) of the original
    data and can generate any number of data points.
    """

    __window_size: int
    __sliding_step: int

    def __init__(self, window_size: int, sliding_step: int):
        """
        Constructor. You need to specify the number of rows in each sliding size window (except for the last window) and
        the sliding step to the next window.

        Example
        Original data points (time, s1, s2):
        (1, 100, null )
        (2, 100, null )
        (3, 100, null )
        (4, 100, 'error')
        (5, 100, null )
        (6, 101, 'error')
        (7, 102, 'error')

        Set windowSize to 2 and set slidingStep to 3, windows will be generated as below: Window 0: [(1, 100, null ),
        (2, 100, null)] Window 1: [(4, 100, 'error'), (5, 100, null)] Window 2: [(7, 102, 'error')]

        :param window_size: the number of rows in each sliding size window (0 < window_size)
        :param sliding_step: the number of rows between the first point of the next window and the first point of the
        current window (0 < sliding_step)
        """
        self.__window_size = window_size
        self.__sliding_step = sliding_step

    def check(self):
        if self.__window_size <= 0:
            raise UDFException(
                "Parameter window_size({}) should be positive.".format(
                    self.__window_size
                )
            )
        if 2147483647 < self.__window_size:
            raise UDFException(
                "Parameter window_size({}) should be less than or equals to 2147483647.".format(
                    self.__window_size
                )
            )

        if self.__sliding_step <= 0:
            raise UDFException(
                "Parameter sliding_step({}) should be positive.".format(
                    self.__sliding_step
                )
            )
        if 2147483647 < self.__sliding_step:
            raise UDFException(
                "Parameter sliding_step({}) should be less than or equals to 2147483647.".format(
                    self.__sliding_step
                )
            )

    def get_access_strategy_type(self) -> AccessStrategyType:
        return AccessStrategyType.SLIDING_SIZE_WINDOW

    def get_window_size(self) -> int:
        return self.__window_size

    def get_sliding_step(self) -> int:
        return self.__sliding_step
