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
import re

from iotdb.ainode.core.model.model_constants import (
    MODEL_CONFIG_FILE_IN_YAML,
    MODEL_WEIGHTS_FILE_IN_PT,
)


class _BaseException(Exception):
    """Base class for exceptions in this module."""

    def __init__(self):
        self.message = None

    def __str__(self) -> str:
        return self.message


class BadNodeUrlException(_BaseException):
    def __init__(self, node_url: str):
        super().__init__()
        self.message = "Bad node url: {}".format(node_url)


# ==================== Model Management ====================


class ModelExistedException(_BaseException):
    def __init__(self, model_id: str):
        super().__init__()
        self.message = "Model {} already exists".format(model_id)


class ModelNotExistException(_BaseException):
    def __init__(self, model_id: str):
        super().__init__()
        self.message = "Model {} does not exist".format(model_id)


class InvalidModelUriException(_BaseException):
    def __init__(self, msg: str):
        super().__init__()
        self.message = (
            "Model registration failed because the specified uri is invalid: {}".format(
                msg
            )
        )


class BuiltInModelDeletionException(_BaseException):
    def __init__(self, model_id: str):
        super().__init__()
        self.message = "Cannot delete built-in model: {}".format(model_id)


class BadConfigValueException(_BaseException):
    def __init__(self, config_name: str, config_value, hint: str = ""):
        super().__init__()
        self.message = "Bad value [{0}] for config {1}. {2}".format(
            config_value, config_name, hint
        )


class InferenceModelInternalException(_BaseException):
    def __init__(self, msg: str):
        super().__init__()
        self.message = "Inference model internal error: {0}".format(msg)


class BuiltInModelNotSupportException(_BaseException):
    def __init__(self, msg: str):
        super().__init__()
        self.message = "Built-in model not support: {0}".format(msg)


class WrongAttributeTypeException(_BaseException):
    def __init__(self, attribute_name: str, expected_type: str):
        super().__init__()
        self.message = "Wrong type for attribute: {0}, expected: {1}".format(
            attribute_name, expected_type
        )


class NumericalRangeException(_BaseException):
    def __init__(self, attribute_name: str, value, min_value, max_value):
        super().__init__()
        self.message = (
            "Attribute {0} expect value between {1} and {2}, got {3} instead.".format(
                attribute_name, min_value, max_value, value
            )
        )


class StringRangeException(_BaseException):
    def __init__(self, attribute_name: str, value: str, expect_value):
        super().__init__()
        self.message = "Attribute {0} expect value in {1}, got {2} instead.".format(
            attribute_name, expect_value, value
        )


class ListRangeException(_BaseException):
    def __init__(self, attribute_name: str, value: list, expected_type: str):
        super().__init__()
        self.message = (
            "Attribute {0} expect value type list[{1}], got {2} instead.".format(
                attribute_name, expected_type, value
            )
        )
