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

from ainode.core.constant import DEFAULT_CONFIG_FILE_NAME, DEFAULT_MODEL_FILE_NAME


class _BaseError(Exception):
    """Base class for exceptions in this module."""

    def __init__(self):
        self.message = None

    def __str__(self) -> str:
        return self.message


class BadNodeUrlError(_BaseError):
    def __init__(self, node_url: str):
        self.message = "Bad node url: {}".format(node_url)


class ModelNotExistError(_BaseError):
    def __init__(self, file_path: str):
        self.message = "Model path is not exists: {} ".format(file_path)


class BadConfigValueError(_BaseError):
    def __init__(self, config_name: str, config_value, hint: str = ""):
        self.message = "Bad value [{0}] for config {1}. {2}".format(
            config_value, config_name, hint
        )


class MissingConfigError(_BaseError):
    def __init__(self, config_name: str):
        self.message = "Missing config: {}".format(config_name)


class MissingOptionError(_BaseError):
    def __init__(self, config_name: str):
        self.message = "Missing task option: {}".format(config_name)


class RedundantOptionError(_BaseError):
    def __init__(self, option_name: str):
        self.message = "Redundant task option: {}".format(option_name)


class WrongTypeConfigError(_BaseError):
    def __init__(self, config_name: str, expected_type: str):
        self.message = "Wrong type for config: {0}, expected: {1}".format(
            config_name, expected_type
        )


class UnsupportedError(_BaseError):
    def __init__(self, msg: str):
        self.message = "{0} is not supported in current version".format(msg)



class InvalidUriError(_BaseError):
    def __init__(self, uri: str, details: str = ""):
        if details:
            self.message = "Invalid uri: {}, {}".format(uri, details)
        else:
            # 修复硬编码错误
            self.message = "Invalid uri: {}, no valid model files found (checked both IoTDB and legacy formats)".format(uri)



class InvalidWindowArgumentError(_BaseError):
    def __init__(self, window_interval, window_step, dataset_length):
        self.message = f"Invalid inference input: window_interval {window_interval}, window_step {window_step}, dataset_length {dataset_length}"


class InferenceModelInternalError(_BaseError):
    def __init__(self, msg: str):
        self.message = "Inference model internal error: {0}".format(msg)


class BuiltInModelNotSupportError(_BaseError):
    def __init__(self, msg: str):
        self.message = "Built-in model not support: {0}".format(msg)


class WrongAttributeTypeError(_BaseError):
    def __init__(self, attribute_name: str, expected_type: str):
        self.message = "Wrong type for attribute: {0}, expected: {1}".format(
            attribute_name, expected_type
        )


class NumericalRangeException(_BaseError):
    def __init__(self, attribute_name: str, value, min_value, max_value):
        self.message = (
            "Attribute {0} expect value between {1} and {2}, got {3} instead.".format(
                attribute_name, min_value, max_value, value
            )
        )


class StringRangeException(_BaseError):
    def __init__(self, attribute_name: str, value: str, expect_value):
        self.message = "Attribute {0} expect value in {1}, got {2} instead.".format(
            attribute_name, expect_value, value
        )


class ListRangeException(_BaseError):
    def __init__(self, attribute_name: str, value: list, expected_type: str):
        self.message = (
            "Attribute {0} expect value type list[{1}], got {2} instead.".format(
                attribute_name, expected_type, value
            )
        )


class AttributeNotSupportError(_BaseError):
    def __init__(self, model_name: str, attribute_name: str):
        self.message = "Attribute {0} is not supported in model {1}".format(
            attribute_name, model_name
        )

# new error
class ModelLoadingError(_BaseError):
    def __init__(self, model_id: str, error_msg: str):
        self.message = f"Failed to load model {model_id}: {error_msg}"


class ModelFormatError(_BaseError):
    def __init__(self, model_path: str, expected_format: str):
        self.message = f"Invalid model format at {model_path}, expected {expected_format}"


class IoTDBModelError(_BaseError):
    def __init__(self, model_type: str, error_msg: str):
        self.message = f"IoTDB model error for {model_type}: {error_msg}"


class UnsupportedModelTypeError(_BaseError):
    def __init__(self, model_type: str):
        self.message = f"Unsupported model type: {model_type}. Supported types: timer, sundial"


class ConfigValidationError(_BaseError):
    def __init__(self, config_path: str, validation_msg: str):
        self.message = f"Configuration validation failed for {config_path}: {validation_msg}"


class WeightFileError(_BaseError):
    def __init__(self, weight_path: str, error_msg: str):
        self.message = f"Weight file error at {weight_path}: {error_msg}"

# This is used to extract the key message in RuntimeError instead of the traceback message
def runtime_error_extractor(error_message):
    pattern = re.compile(r"RuntimeError: (.+)")
    match = pattern.search(error_message)

    if match:
        return match.group(1)
    else:
        return ""
