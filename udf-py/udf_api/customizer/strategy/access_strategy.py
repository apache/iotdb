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

from udf_api.customizer.strategy.access_strategy_type import AccessStrategyType


class AccessStrategy(metaclass=ABCMeta):
    """
    Used to customize the strategy for accessing raw data in UDTF#beforeStart(UDFParameters,
    UDTFConfigurations)}.
    """

    @abstractmethod
    def check(self):
        """
        Used by the system to check the access strategy.

        :raise UDFException: if invalid strategy is set
        """
        pass

    @abstractmethod
    def get_access_strategy_type(self) -> AccessStrategyType:
        """
        Returns the actual access strategy type.

        :return AccessStrategyType: the actual access strategy type
        """
        pass
