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
from enum import Enum
from typing import List


class ForecastTaskType(Enum):
    """
    In multivariable time series forecasting tasks, the columns to be predicted are called the endogenous variables
    and the columns that are independent or non-forecast are called the exogenous variables (they might be helpful
    to predict the endogenous variables). Both of them can appear in the input time series.

    ForecastTaskType.ENDOGENOUS: all input time series of input are endogenous variables
    ForecastTaskType.EXOGENOUS: the input time series is combined with endogenous and exogenous variables
    """
    ENDOGENOUS = "endogenous"
    EXOGENOUS = "exogenous"

    def __str__(self) -> str:
        return self.value

    def __eq__(self, other: str) -> bool:
        return self.value == other

    def __hash__(self) -> int:
        return hash(self.value)


class ForecastModelType(Enum):
    DLINEAR = "dlinear"
    DLINEAR_INDIVIDUAL = "dlinear_individual"
    NBEATS = "nbeats"

    @classmethod
    def values(cls) -> List[str]:
        values = []
        for item in list(cls):
            values.append(item.value)
        return values
