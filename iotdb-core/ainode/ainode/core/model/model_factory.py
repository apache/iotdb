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
import glob
import os
import shutil
from urllib.parse import urljoin

import yaml

from ainode.core.constant import (
    MODEL_CONFIG_FILE_IN_YAML,
    MODEL_WEIGHTS_FILE_IN_PT,
)
from ainode.core.exception import BadConfigValueError, InvalidUriError
from ainode.core.log import Logger
from ainode.core.model.model_info import ModelFileType
from ainode.core.model.uri_utils import (
    UriType,
    download_file,
    download_snapshot_from_hf,
)
from ainode.core.util.serde import get_data_type_byte_from_str
from ainode.thrift.ainode.ttypes import TConfigs

logger = Logger()


def fetch_model_by_uri(
    uri_type: UriType, uri: str, storage_path: str, model_file_type: ModelFileType
):
    """
    Fetch the model files from the specified URI.

    Args:
        uri_type (UriType): type of the URI, either repo, file, http or https
        uri (str): a network or a local path of the model to be registered
        storage_path (str): path to save the whole model, including weights, config, codes, etc.
        model_file_type (ModelFileType): The type of model file, either safetensors or pytorch
    Returns: TODO: Will be removed in future
        configs: TConfigs
        attributes: str
    """
    if uri_type == UriType.REPO or uri_type in [UriType.HTTP, UriType.HTTPS]:
        return _fetch_model_from_network(uri, storage_path, model_file_type)
    elif uri_type == UriType.FILE:
        return _fetch_model_from_local(uri, storage_path, model_file_type)
    else:
        raise InvalidUriError(f"Invalid URI type: {uri_type}")


def _fetch_model_from_network(
    uri: str, storage_path: str, model_file_type: ModelFileType
):
    """
    Returns: TODO: Will be removed in future
        configs: TConfigs
        attributes: str
    """
    if model_file_type == ModelFileType.SAFETENSORS:
        download_snapshot_from_hf(uri, storage_path)
        return _process_huggingface_files(storage_path)

    # TODO: The following codes might be refactored in future
    # concat uri to get complete url
    uri = uri if uri.endswith("/") else uri + "/"
    target_model_path = urljoin(uri, MODEL_WEIGHTS_FILE_IN_PT)
    target_config_path = urljoin(uri, MODEL_CONFIG_FILE_IN_YAML)

    # download config file
    config_storage_path = os.path.join(storage_path, MODEL_CONFIG_FILE_IN_YAML)
    download_file(target_config_path, config_storage_path)

    # read and parse config dict from config.yaml
    with open(config_storage_path, "r", encoding="utf-8") as file:
        config_dict = yaml.safe_load(file)
    configs, attributes = _parse_inference_config(config_dict)

    # if config.yaml is correct, download model file
    model_storage_path = os.path.join(storage_path, MODEL_WEIGHTS_FILE_IN_PT)
    download_file(target_model_path, model_storage_path)
    return configs, attributes


def _fetch_model_from_local(
    uri: str, storage_path: str, model_file_type: ModelFileType
):
    """
    Returns: TODO: Will be removed in future
        configs: TConfigs
        attributes: str
    """
    if model_file_type == ModelFileType.SAFETENSORS:
        # copy anything in the uri to local_dir
        for file in os.listdir(uri):
            shutil.copy(os.path.join(uri, file), storage_path)
        return _process_huggingface_files(storage_path)
    # concat uri to get complete path
    target_model_path = os.path.join(uri, MODEL_WEIGHTS_FILE_IN_PT)
    model_storage_path = os.path.join(storage_path, MODEL_WEIGHTS_FILE_IN_PT)
    target_config_path = os.path.join(uri, MODEL_CONFIG_FILE_IN_YAML)
    config_storage_path = os.path.join(storage_path, MODEL_CONFIG_FILE_IN_YAML)

    # check if file exist
    exist_model_file = os.path.exists(target_model_path)
    exist_config_file = os.path.exists(target_config_path)

    configs = None
    attributes = None
    if exist_model_file and exist_config_file:
        # copy config.yaml
        shutil.copy(target_config_path, config_storage_path)
        logger.info(
            f"copy file from {target_config_path} to {config_storage_path} success"
        )

        # read and parse config dict from config.yaml
        with open(config_storage_path, "r", encoding="utf-8") as file:
            config_dict = yaml.safe_load(file)
        configs, attributes = _parse_inference_config(config_dict)

        # if config.yaml is correct, copy model file
        shutil.copy(target_model_path, model_storage_path)
        logger.info(
            f"copy file from {target_model_path} to {model_storage_path} success"
        )

    elif not exist_model_file or not exist_config_file:
        raise InvalidUriError(uri)

    return configs, attributes


def _parse_inference_config(config_dict):
    """
    Args:
        config_dict: dict
            - configs: dict
                - input_shape (list<i32>): input shape of the model and needs to be two-dimensional array like [96, 2]
                - output_shape (list<i32>): output shape of the model and needs to be two-dimensional array like [96, 2]
                - input_type (list<str>): input type of the model and each element needs to be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text'], default float64
                - output_type (list<str>): output type of the model and each element needs to be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text'], default float64
            - attributes: dict
    Returns:
        configs: TConfigs
        attributes: str
    """
    configs = config_dict["configs"]

    # check if input_shape and output_shape are two-dimensional array
    if not (
        isinstance(configs["input_shape"], list) and len(configs["input_shape"]) == 2
    ):
        raise BadConfigValueError(
            "input_shape",
            configs["input_shape"],
            "input_shape should be a two-dimensional array.",
        )
    if not (
        isinstance(configs["output_shape"], list) and len(configs["output_shape"]) == 2
    ):
        raise BadConfigValueError(
            "output_shape",
            configs["output_shape"],
            "output_shape should be a two-dimensional array.",
        )

    # check if input_shape and output_shape are positive integer
    input_shape_is_positive_number = (
        isinstance(configs["input_shape"][0], int)
        and isinstance(configs["input_shape"][1], int)
        and configs["input_shape"][0] > 0
        and configs["input_shape"][1] > 0
    )
    if not input_shape_is_positive_number:
        raise BadConfigValueError(
            "input_shape",
            configs["input_shape"],
            "element in input_shape should be positive integer.",
        )

    output_shape_is_positive_number = (
        isinstance(configs["output_shape"][0], int)
        and isinstance(configs["output_shape"][1], int)
        and configs["output_shape"][0] > 0
        and configs["output_shape"][1] > 0
    )
    if not output_shape_is_positive_number:
        raise BadConfigValueError(
            "output_shape",
            configs["output_shape"],
            "element in output_shape should be positive integer.",
        )

    # check if input_type and output_type are one-dimensional array with right length
    if "input_type" in configs and not (
        isinstance(configs["input_type"], list)
        and len(configs["input_type"]) == configs["input_shape"][1]
    ):
        raise BadConfigValueError(
            "input_type",
            configs["input_type"],
            "input_type should be a one-dimensional array and length of it should be equal to input_shape[1].",
        )

    if "output_type" in configs and not (
        isinstance(configs["output_type"], list)
        and len(configs["output_type"]) == configs["output_shape"][1]
    ):
        raise BadConfigValueError(
            "output_type",
            configs["output_type"],
            "output_type should be a one-dimensional array and length of it should be equal to output_shape[1].",
        )

    # parse input_type and output_type to byte
    if "input_type" in configs:
        input_type = [get_data_type_byte_from_str(x) for x in configs["input_type"]]
    else:
        input_type = [get_data_type_byte_from_str("float32")] * configs["input_shape"][
            1
        ]

    if "output_type" in configs:
        output_type = [get_data_type_byte_from_str(x) for x in configs["output_type"]]
    else:
        output_type = [get_data_type_byte_from_str("float32")] * configs[
            "output_shape"
        ][1]

    # parse attributes
    attributes = ""
    if "attributes" in config_dict:
        attributes = str(config_dict["attributes"])

    return (
        TConfigs(
            configs["input_shape"], configs["output_shape"], input_type, output_type
        ),
        attributes,
    )


def _process_huggingface_files(local_dir: str):
    """
    TODO: Currently, we use this function to convert the model config from huggingface, we will refactor this in the future.
    """
    config_file = None
    for config_name in ["config.json", "model_config.json"]:
        config_path = os.path.join(local_dir, config_name)
        if os.path.exists(config_path):
            config_file = config_path
            break

    if not config_file:
        raise InvalidUriError(f"No config.json found in {local_dir}")

    safetensors_files = glob.glob(os.path.join(local_dir, "*.safetensors"))
    if not safetensors_files:
        raise InvalidUriError(f"No .safetensors files found in {local_dir}")

    simple_config = {
        "configs": {
            "input_shape": [96, 1],
            "output_shape": [96, 1],
            "input_type": ["float32"],
            "output_type": ["float32"],
        },
        "attributes": {
            "model_type": "huggingface_model",
            "source_dir": local_dir,
            "files": [os.path.basename(f) for f in safetensors_files],
        },
    }

    configs, attributes = _parse_inference_config(simple_config)
    return configs, attributes
