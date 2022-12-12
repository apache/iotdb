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


class MappableRowByRowAccessStrategy(AccessStrategy):
    """
    Used in UDTF#beforeStart(UDFParameters, UDTFConfigurations).

    When the access strategy of a UDTF is set to an instance of this class, the method UDTF#transform(Row) of the UDTF
    will be called to transform the original data. You need to override the method in your own UDTF class.

    Each call of the method UDTF#transform(Row) processes only one row (aligned by time) of the original data and can
    generate any number of data points.
    """

    def check(self):
        """
        nothing needs to check
        """
        pass

    def get_access_strategy_type(self) -> AccessStrategyType:
        return AccessStrategyType.MAPPABLE_ROW_BY_ROW
