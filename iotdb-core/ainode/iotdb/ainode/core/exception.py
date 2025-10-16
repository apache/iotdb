1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1import re
1
1from iotdb.ainode.core.constant import (
1    MODEL_CONFIG_FILE_IN_YAML,
1    MODEL_WEIGHTS_FILE_IN_PT,
1)
1
1
1class _BaseError(Exception):
1    """Base class for exceptions in this module."""
1
1    def __init__(self):
1        self.message = None
1
1    def __str__(self) -> str:
1        return self.message
1
1
1class BadNodeUrlError(_BaseError):
1    def __init__(self, node_url: str):
1        self.message = "Bad node url: {}".format(node_url)
1
1
1class ModelNotExistError(_BaseError):
1    def __init__(self, msg: str):
1        self.message = "Model is not exists: {} ".format(msg)
1
1
1class BadConfigValueError(_BaseError):
1    def __init__(self, config_name: str, config_value, hint: str = ""):
1        self.message = "Bad value [{0}] for config {1}. {2}".format(
1            config_value, config_name, hint
1        )
1
1
1class MissingConfigError(_BaseError):
1    def __init__(self, config_name: str):
1        self.message = "Missing config: {}".format(config_name)
1
1
1class MissingOptionError(_BaseError):
1    def __init__(self, config_name: str):
1        self.message = "Missing task option: {}".format(config_name)
1
1
1class RedundantOptionError(_BaseError):
1    def __init__(self, option_name: str):
1        self.message = "Redundant task option: {}".format(option_name)
1
1
1class WrongTypeConfigError(_BaseError):
1    def __init__(self, config_name: str, expected_type: str):
1        self.message = "Wrong type for config: {0}, expected: {1}".format(
1            config_name, expected_type
1        )
1
1
1class UnsupportedError(_BaseError):
1    def __init__(self, msg: str):
1        self.message = "{0} is not supported in current version".format(msg)
1
1
1class InvalidUriError(_BaseError):
1    def __init__(self, uri: str):
1        self.message = "Invalid uri: {}, there are no {} or {} under this uri.".format(
1            uri, MODEL_WEIGHTS_FILE_IN_PT, MODEL_CONFIG_FILE_IN_YAML
1        )
1
1
1class InvalidWindowArgumentError(_BaseError):
1    def __init__(self, window_interval, window_step, dataset_length):
1        self.message = f"Invalid inference input: window_interval {window_interval}, window_step {window_step}, dataset_length {dataset_length}"
1
1
1class InferenceModelInternalError(_BaseError):
1    def __init__(self, msg: str):
1        self.message = "Inference model internal error: {0}".format(msg)
1
1
1class BuiltInModelNotSupportError(_BaseError):
1    def __init__(self, msg: str):
1        self.message = "Built-in model not support: {0}".format(msg)
1
1
1class BuiltInModelDeletionError(_BaseError):
1    def __init__(self, model_id: str):
1        self.message = "Cannot delete built-in model: {0}".format(model_id)
1
1
1class WrongAttributeTypeError(_BaseError):
1    def __init__(self, attribute_name: str, expected_type: str):
1        self.message = "Wrong type for attribute: {0}, expected: {1}".format(
1            attribute_name, expected_type
1        )
1
1
1class NumericalRangeException(_BaseError):
1    def __init__(self, attribute_name: str, value, min_value, max_value):
1        self.message = (
1            "Attribute {0} expect value between {1} and {2}, got {3} instead.".format(
1                attribute_name, min_value, max_value, value
1            )
1        )
1
1
1class StringRangeException(_BaseError):
1    def __init__(self, attribute_name: str, value: str, expect_value):
1        self.message = "Attribute {0} expect value in {1}, got {2} instead.".format(
1            attribute_name, expect_value, value
1        )
1
1
1class ListRangeException(_BaseError):
1    def __init__(self, attribute_name: str, value: list, expected_type: str):
1        self.message = (
1            "Attribute {0} expect value type list[{1}], got {2} instead.".format(
1                attribute_name, expected_type, value
1            )
1        )
1
1
1class AttributeNotSupportError(_BaseError):
1    def __init__(self, model_name: str, attribute_name: str):
1        self.message = "Attribute {0} is not supported in model {1}".format(
1            attribute_name, model_name
1        )
1
1
1# This is used to extract the key message in RuntimeError instead of the traceback message
1def runtime_error_extractor(error_message):
1    pattern = re.compile(r"RuntimeError: (.+)")
1    match = pattern.search(error_message)
1
1    if match:
1        return match.group(1)
1    else:
1        return ""
1