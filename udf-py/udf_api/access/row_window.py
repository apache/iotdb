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


from abc import ABCMeta, abstractmethod

from udf_api.access.row import Row
from udf_api.access.row_iterator import RowIterator
from udf_api.type.type import Type


class RowWindow(metaclass=ABCMeta):
    @abstractmethod
    def window_size(self):
        """
        Returns the number of rows in this window.

        :return: the number of rows in this window
        """
        pass

    @abstractmethod
    def get_row(self, row_index: int) -> Row:
        """
        Returns the row at the specified position in this window.

        Note that the Row instance returned by this method each time is the same instance. In other words, calling this
        method will only change the member variables inside the Row instance, but will not generate a new Row instance.

        :return: the row at the specified position in this window.
        """
        pass

    @abstractmethod
    def get_data_type(self, column_index: int) -> Type:
        """
        Returns the actual data type of the values at the specified column in this window.

        :return: the actual data type of the values at the specified column in this window
        """
        pass

    @abstractmethod
    def get_row_iterator(self) -> RowIterator:
        """
        Returns an iterator used to access this window.

        :return: an iterator used to access this window
        """
        pass

    @abstractmethod
    def window_start_time(self) -> int:
        """
        For different types of windows, the definition of the window start time is different.
        For sliding size window: the window start time is equal to the timestamp of the first row.
        For sliding time window: The window start time is determined by display_window_begin
        SlidingTimeWindowAccessStrategy#get_display_window_begin() and
        slidingStep SlidingTimeWindowAccessStrategy#get_sliding_step().
        The window start time for the i-th window (i starts at 0) can be calculated as
        display_window_begin + i * sliding_step.

        :return: the start time of the window
        """
        pass

    @abstractmethod
    def window_start_end(self) -> int:
        """
        For different types of windows, the definition of the window end time is different.
        For sliding size window: the window end time is equal to the timestamp of the last row.
        For sliding time window: The window end time is determined by display_window_begin
        SlidingTimeWindowAccessStrategy#get_display_window_begin(), time_interval
        SlidingTimeWindowAccessStrategy#get_time_interval() and sliding_step
        SlidingTimeWindowAccessStrategy#get_sliding_step(). The window end time for the i-th window (i starts at 0) can
        be calculated as display_window_begin + i * sliding_step + time_interval - 1 or
        window_start_time(i) + time_interval - 1.

        :return: the end time of the window
        """
        pass
