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
from abc import abstractmethod

from udf_api.access.row import Row
from udf_api.access.row_window import RowWindow
from udf_api.collector.point_collector import PointCollector
from udf_api.customizer.config.udtf_configurations import UDTFConfigurations
from udf_api.customizer.parameter.udf_parameters import UDFParameters
from udf_api.udf import UDF


class UDTF(UDF):
    """
    User-defined Time-series Generating Function (UDTF)

    New UDTF classes need to inherit from this UDTF class.

    Generates a variable number of output data points for a single input row or a single input window (time-based or
    size-based).

    A complete UDTF needs to override at least the following methods:
    * beforeStart(UDFParameters, UDTFConfigurations)
    * transform(RowWindow, PointCollector) or transform(Row, PointCollector)

    In the life cycle of a UDTF instance, the calling sequence of each method is as follows:
    1. validate(UDFParameterValidator)
    2. beforeStart(UDFParameters, UDTFConfigurations)
    3. transform(RowWindow, PointCollector) or transform(Row, PointCollector)
    4. terminate(PointCollector) 5. beforeDestroy()

    The query engine will instantiate an independent UDTF instance for each udf query column, and different UDTF
    instances will not affect each other.
    """

    @abstractmethod
    def before_start(
        self, parameters: UDFParameters, configurations: UDTFConfigurations
    ):
        """
        This method is mainly used to customize UDTF. In this method, the user can do the following things:
        * Use UDFParameters to get the time series paths and parse key-value pair attributes entered by the user.
        * Set the strategy to access the original data and set the output data type in UDTFConfigurations.
        * Create resources, such as establishing external connections, opening files, etc.

        This method is called after the UDTF is instantiated and before the beginning of the transformation process.

        :param parameters: used to parse the input parameters entered by the user
        :param configurations: used to set the required properties in the UDTF
        """
        pass

    def transform_row(self, row: Row, collector: PointCollector):
        """
        When the user specifies RowByRowAccessStrategy to access the original data in UDTFConfigurations, this method
        will be called to process the transformation. In a single UDF query, this method may be called multiple times.

        :param row: original input data row (aligned by time)
        :param collector: used to collect output data points
        """
        pass

    def transform_window(self, row_window: RowWindow, collector: PointCollector):
        """
        When the user specifies SlidingSizeWindowAccessStrategy or SlidingTimeWindowAccessStrategy to access the
        original data in UDTFConfigurations, this method will be called to process the transformation. In a single UDF
        query, this method may be called multiple times.

        :param row_window: original input data window (rows inside the window are aligned by time)
        :param collector: used to collect output data points
        """

    def map_row(self, row: Row) -> object:
        """
        When the user specifies MappableRowByRowAccessStrategy to access the original data in UDTFConfigurations, this
        method will be called to process the transformation. In a single UDF query, this method may be called multiple
        times.

        :param row: original input data row (aligned by time)
        """
        pass

    def terminate(self, collector: PointCollector):
        """
        This method will be called once after all calls or { UDTF#transform(RowWindow, PointCollector) calls have been
        executed. In a single UDF query, this method will and will only be called once.

        :param collector: used to collect output data points
        """
        pass
