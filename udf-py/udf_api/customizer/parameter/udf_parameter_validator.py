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

from typing import List

from udf_api.customizer.parameter.udf_parameters import UDFParameters
from udf_api.exception.udf_attribute_not_provided_exception import (
    UDFAttributeNotProvidedException,
)
from udf_api.exception.udf_input_series_data_type_not_valid_exception import (
    UDFInputSeriesDataTypeNotValidException,
)
from udf_api.exception.udf_input_series_index_not_valid_exception import (
    UDFInputSeriesIndexNotValidException,
)
from udf_api.exception.udf_input_series_number_not_valid_exception import (
    UDFInputSeriesNumberNotValidException,
)
from udf_api.exception.udf_parameter_not_valid_exception import (
    UDFParameterNotValidException,
)
from udf_api.type.type import Type


class UDFParameterValidator:
    __parameters: UDFParameters

    def __init__(self, parameters: UDFParameters):
        self.__parameters = parameters

    def get_parameters(self) -> UDFParameters:
        return self.__parameters

    def validate_required_attribute(self, attribute_key: str):
        """
        Validates whether the attributes entered by the user contain an attribute whose key is
        attributeKey.

        :param attribute_key: key of the attribute
        :raise UDFAttributeNotProvidedException: if the attribute is not provided
        """
        if not self.__parameters.has_attribute(attribute_key):
            raise UDFAttributeNotProvidedException(attribute_key)

        return self

    def validate_input_series_data_type(
        self, index: int, expected_data_types: List[Type]
    ):
        """
        Validates whether the data type of the input series at the specified column is as expected.

        :param index: index of the specified column
        :param expected_data_types: the expected data types
        :raise UDFInputSeriesIndexNotValidException: if the index of the specified column is out of bound
        :raise UDFInputSeriesDataTypeNotValidException: if the data type of the input series at the specified column is
        not as expected
        """
        self.validate_input_series_index(index)

        actual_data_type = self.__parameters.get_child_expression_data_types()[index]

        for expected_data_type in expected_data_types:
            if expected_data_type == actual_data_type:
                return self

        raise UDFInputSeriesDataTypeNotValidException(
            index, actual_data_type, expected_data_types
        )

    def validate_input_series_number(
        self,
        expected_series_number_lower_bound: int,
        expected_series_number_upper_bound,
    ):
        """
        Validates whether the number of the input series is as expected.

        :param expected_series_number_lower_bound: the number of the input series must be greater than or equal to the
        expected_series_number_lower_bound
        :param expected_series_number_upper_bound: the number of the input series must be less than or equal to the
        expected_series_number_upper_bound
        :raise UDFInputSeriesNumberNotValidException: if the number of the input series is not as expected
        """
        actual_series_number = self.__parameters.get_child_expressions_count()
        if (
            actual_series_number < expected_series_number_lower_bound
            or expected_series_number_upper_bound < actual_series_number
        ):
            raise UDFInputSeriesNumberNotValidException(
                actual_series_number,
                expected_series_number_lower_bound,
                expected_series_number_upper_bound,
            )

        return self

    def validate(self, message_to_raise: str, validation_rule, **kwargs):
        """
        Validates the input parameters according to the validation rule given by the user.

        :param message_to_raise: the message to throw when the given arguments are not valid
        :param validation_rule: the validation rule, which can be a lambda expression
        :param kwargs: the given arguments
        :raise UDFParameterNotValidException: if the given arguments are not valid
        """
        if not validation_rule(kwargs):
            raise UDFParameterNotValidException(message_to_raise)

        return self

    def validate_input_series_index(self, index: int):
        """
        Validates whether the index of the specified column is out of bound. bound: [0,
        self.__parameters.get_child_expressions_count())

        :param index: the index of the specified column
        :raise UDFInputSeriesIndexNotValidException: if the index of the specified column is out of bound
        """
        actual_series_number = self.__parameters.get_child_expressions_count()
        if index < 0 or actual_series_number <= index:
            raise UDFInputSeriesIndexNotValidException(index, actual_series_number)
