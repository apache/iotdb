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

from typing import Dict, List

from udf_api.type.type import Type


# Used in UDTF#beforeStart(UDFParameters, UDTFConfigurations).
# This class is used to parse the parameters in the UDF entered by the user.
# The input parameters of UDF have two parts. The first part is the paths (measurements) of the time series that the UDF
# needs to process, and the second part is the key-value pair attributes for customization. Only the second part can be
# empty.
# Note that the user must enter the paths (measurements) part before entering the attributes part.
class UDFParameters:
    __child_expressions: List[str]
    __child_expression_data_types: List[Type]
    __child_expressions_count: int
    __attributes: Dict[str, str]

    def __init__(
        self,
        child_expressions: List[str],
        child_expression_data_types: List[Type],
        attributes: Dict[str, str],
    ):
        self.__child_expressions = child_expressions
        self.__child_expression_data_types = child_expression_data_types
        self.__child_expressions_count = len(child_expressions)
        self.__attributes = attributes

    def get_child_expressions(self) -> List[str]:
        return self.__child_expressions

    def get_child_expression_data_types(self) -> List[Type]:
        return self.__child_expression_data_types

    def get_attributes(self) -> Dict[str, str]:
        return self.__attributes

    def get_child_expressions_count(self) -> int:
        return self.__child_expressions_count

    def has_attribute(self, key: str) -> bool:
        return key in self.__attributes

    def get_string(self, key: str, default: str = None) -> str:
        string_value = self.__attributes[key]
        return default if string_value is None else string_value

    def get_int(self, key: str, default: int = None) -> int:
        string_value = self.get_string(key)
        return default if string_value is None else int(string_value)

    def get_float(self, key: str, default: float = None) -> float:
        string_value = self.get_string(key)
        return default if string_value is None else float(string_value)
