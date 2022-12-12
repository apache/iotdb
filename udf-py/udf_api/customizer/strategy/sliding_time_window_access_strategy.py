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


class SlidingTimeWindowAccessStrategy(AccessStrategy):
    """
    Used in UDTF#beforeStart(UDFParameters, UDTFConfigurations).

    When the access strategy of a UDTF is set to an instance of this class, the method UDTF#transform(RowWindow,
    PointCollector) of the UDTF will be called to transform the original data. You need to override the method in your
    own UDTF class.

    Sliding time window is a kind of time-based window. To partition the raw query data set into sliding time windows,
    you need to give the following 4 parameters:

    * display window begin: determines the start time of the first window
    * display window end: if the start time of current window + sliding step > display window end, then current window
      is the last window that your UDTF can process
    * time interval: determines the time range of a window
    * sliding step: the start time of the next window = the start time of current window + sliding step

    Each call of the method UDTF.transform(RowWindow, PointCollector) processes one time window and can generate any
    number of data points. Note that the transform method will still be called when there is no data point in a window.
    Note that the time range of the last few windows may be less than the specified time interval.
    """

    __time_interval: int
    __sliding_step: int
    __display_window_begin: int
    __display_window_end: int

    def __init__(
        self,
        time_interval: int,
        sliding_step: int,
        display_window_begin: int,
        display_window_end: int,
    ):
        """
        :param time_interval: 0 < time_interval
        :param sliding_step: 0 < sliding_step
        :param display_window_begin: display_window_begin < display_window_end
        :param display_window_end: display_window_begin < display_window_end
        """
        self.__time_interval = time_interval
        self.__sliding_step = sliding_step
        self.__display_window_begin = display_window_begin
        self.__display_window_end = display_window_end

    def check(self):
        if self.__time_interval <= 0:
            raise UDFException(
                "Parameter time_interval({}) should be positive.".format(
                    self.__time_interval
                )
            )
        if 9223372036854775807 < self.__time_interval:
            raise UDFException(
                "Parameter window_size({}) should be less than or equals to 9223372036854775807.".format(
                    self.__time_interval
                )
            )

        if self.__sliding_step <= 0:
            raise UDFException(
                "Parameter sliding_step({}) should be positive.".format(
                    self.__sliding_step
                )
            )
        if 9223372036854775807 < self.__sliding_step:
            raise UDFException(
                "Parameter sliding_step({}) should be less than or equals to 9223372036854775807.".format(
                    self.__sliding_step
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
                "Parameter display_window_end({}) should be less than or equals to 9223372036854775807.".format(
                    self.__display_window_end
                )
            )
        if self.__display_window_end < self.__display_window_begin:
            raise UDFException(
                "Parameter display_window_end({}) < display_window_begin({})".format(
                    self.__display_window_end, self.__display_window_begin
                )
            )

    def get_access_strategy_type(self) -> AccessStrategyType:
        return AccessStrategyType.SLIDING_TIME_WINDOW

    def get_time_interval(self) -> int:
        return self.__time_interval

    def get_sliding_step(self) -> int:
        return self.__sliding_step

    def get_display_window_begin(self) -> int:
        return self.__display_window_begin

    def get_display_window_end(self) -> int:
        return self.__display_window_end
