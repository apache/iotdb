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
        self.message = "Bad node url: {}".format(node_url)


# ==================== Model Management ====================


class ModelExistedException(_BaseException):
    def __init__(self, model_id: str):
        self.message = "Model {} already exists".format(model_id)


class ModelNotExistException(_BaseException):
    def __init__(self, model_id: str):
        self.message = "Model {} is not exists".format(model_id)


class InvalidModelUriException(_BaseException):
    def __init__(self, msg: str):
        self.message = (
            "Model registration failed because the specified uri is invalid: {}".format(
                msg
            )
        )


class BuiltInModelDeletionException(_BaseException):
    def __init__(self, model_id: str):
        self.message = "Cannot delete built-in model: {}".format(model_id)


class BadConfigValueException(_BaseException):
    def __init__(self, config_name: str, config_value, hint: str = ""):
        self.message = "Bad value [{0}] for config {1}. {2}".format(
            config_value, config_name, hint
        )


class MissingConfigException(_BaseException):
    def __init__(self, config_name: str):
        self.message = "Missing config: {}".format(config_name)


class MissingOptionException(_BaseException):
    def __init__(self, config_name: str):
        self.message = "Missing task option: {}".format(config_name)


class RedundantOptionException(_BaseException):
    def __init__(self, option_name: str):
        self.message = "Redundant task option: {}".format(option_name)


class WrongTypeConfigException(_BaseException):
    def __init__(self, config_name: str, expected_type: str):
        self.message = "Wrong type for config: {0}, expected: {1}".format(
            config_name, expected_type
        )


class UnsupportedException(_BaseException):
    def __init__(self, msg: str):
        self.message = "{0} is not supported in current version".format(msg)


class InvalidUriException(_BaseException):
    def __init__(self, uri: str):
        self.message = "Invalid uri: {}, there are no {} or {} under this uri.".format(
            uri, MODEL_WEIGHTS_FILE_IN_PT, MODEL_CONFIG_FILE_IN_YAML
        )


class InvalidWindowArgumentException(_BaseException):
    def __init__(self, window_interval, window_step, dataset_length):
        self.message = f"Invalid inference input: window_interval {window_interval}, window_step {window_step}, dataset_length {dataset_length}"


class InferenceModelInternalException(_BaseException):
    def __init__(self, msg: str):
        self.message = "Inference model internal error: {0}".format(msg)


class BuiltInModelNotSupportException(_BaseException):
    def __init__(self, msg: str):
        self.message = "Built-in model not support: {0}".format(msg)


class WrongAttributeTypeException(_BaseException):
    def __init__(self, attribute_name: str, expected_type: str):
        self.message = "Wrong type for attribute: {0}, expected: {1}".format(
            attribute_name, expected_type
        )


class NumericalRangeException(_BaseException):
    def __init__(self, attribute_name: str, value, min_value, max_value):
        self.message = (
            "Attribute {0} expect value between {1} and {2}, got {3} instead.".format(
                attribute_name, min_value, max_value, value
            )
        )


class StringRangeException(_BaseException):
    def __init__(self, attribute_name: str, value: str, expect_value):
        self.message = "Attribute {0} expect value in {1}, got {2} instead.".format(
            attribute_name, expect_value, value
        )


class ListRangeException(_BaseException):
    def __init__(self, attribute_name: str, value: list, expected_type: str):
        self.message = (
            "Attribute {0} expect value type list[{1}], got {2} instead.".format(
                attribute_name, expected_type, value
            )
        )


class AttributeNotSupportException(_BaseException):
    def __init__(self, model_name: str, attribute_name: str):
        self.message = "Attribute {0} is not supported in model {1}".format(
            attribute_name, model_name
        )


# This is used to extract the key message in RuntimeError instead of the traceback message
def runtime_error_extractor(error_message):
    pattern = re.compile(r"RuntimeError: (.+)")
    match = pattern.search(error_message)

    if match:
        return match.group(1)
    else:
        return ""
