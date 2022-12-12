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

from udf_api.customizer.config.udf_configurations import UDFConfigurations
from udf_api.customizer.strategy.access_strategy import AccessStrategy
from udf_api.exception.udf_exception import UDFException
from udf_api.type.type import Type


class UDTFConfigurations(UDFConfigurations):
    """
    Used in UDTF#beforeStart(UDFParameters, UDTFConfigurations).
    Supports calling methods in a chain.
    """

    __access_strategy: AccessStrategy

    def set_access_strategy(self, access_strategy: AccessStrategy):
        """
        Used to specify the strategy for accessing raw query data in UDTF.

        :param access_strategy: the specified access strategy. it should be an instance of AccessStrategy.
        :return: self
        """
        self.__access_strategy = access_strategy
        return self

    def get_access_strategy(self) -> AccessStrategy:
        return self.__access_strategy

    def set_output_data_type(self, output_data_type: Type):
        """
        Used to specify the output data type of the UDTF.

        In other words, the data type you set here determines the type of data that the PointCollector in
        UDTF#transform(Row, PointCollector), UDTF#transform(RowWindow, PointCollector) or UDTF#terminate(PointCollector)
        can receive.

        :param output_data_type: the output data type of the UDTF
        :return: self
        """
        self._output_data_type = output_data_type
        return self

    def check(self):
        super().check()

        if self.__access_strategy is None:
            raise UDFException("Access strategy is not set.")
        self.__access_strategy.check()
