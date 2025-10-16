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
1import glob
1import os
1import shutil
1from urllib.parse import urljoin
1
1import yaml
1
1from iotdb.ainode.core.constant import (
1    MODEL_CONFIG_FILE_IN_YAML,
1    MODEL_WEIGHTS_FILE_IN_PT,
1)
1from iotdb.ainode.core.exception import BadConfigValueError, InvalidUriError
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.model.model_enums import ModelFileType
1from iotdb.ainode.core.model.uri_utils import (
1    UriType,
1    download_file,
1    download_snapshot_from_hf,
1)
1from iotdb.ainode.core.util.serde import get_data_type_byte_from_str
1from iotdb.thrift.ainode.ttypes import TConfigs
1
1logger = Logger()
1
1
1def fetch_model_by_uri(
1    uri_type: UriType, uri: str, storage_path: str, model_file_type: ModelFileType
1):
1    """
1    Fetch the model files from the specified URI.
1
1    Args:
1        uri_type (UriType): type of the URI, either repo, file, http or https
1        uri (str): a network or a local path of the model to be registered
1        storage_path (str): path to save the whole model, including weights, config, codes, etc.
1        model_file_type (ModelFileType): The type of model file, either safetensors or pytorch
1    Returns: TODO: Will be removed in future
1        configs: TConfigs
1        attributes: str
1    """
1    if uri_type == UriType.REPO or uri_type in [UriType.HTTP, UriType.HTTPS]:
1        return _fetch_model_from_network(uri, storage_path, model_file_type)
1    elif uri_type == UriType.FILE:
1        return _fetch_model_from_local(uri, storage_path, model_file_type)
1    else:
1        raise InvalidUriError(f"Invalid URI type: {uri_type}")
1
1
1def _fetch_model_from_network(
1    uri: str, storage_path: str, model_file_type: ModelFileType
1):
1    """
1    Returns: TODO: Will be removed in future
1        configs: TConfigs
1        attributes: str
1    """
1    if model_file_type == ModelFileType.SAFETENSORS:
1        download_snapshot_from_hf(uri, storage_path)
1        return _process_huggingface_files(storage_path)
1
1    # TODO: The following codes might be refactored in future
1    # concat uri to get complete url
1    uri = uri if uri.endswith("/") else uri + "/"
1    target_model_path = urljoin(uri, MODEL_WEIGHTS_FILE_IN_PT)
1    target_config_path = urljoin(uri, MODEL_CONFIG_FILE_IN_YAML)
1
1    # download config file
1    config_storage_path = os.path.join(storage_path, MODEL_CONFIG_FILE_IN_YAML)
1    download_file(target_config_path, config_storage_path)
1
1    # read and parse config dict from config.yaml
1    with open(config_storage_path, "r", encoding="utf-8") as file:
1        config_dict = yaml.safe_load(file)
1    configs, attributes = _parse_inference_config(config_dict)
1
1    # if config.yaml is correct, download model file
1    model_storage_path = os.path.join(storage_path, MODEL_WEIGHTS_FILE_IN_PT)
1    download_file(target_model_path, model_storage_path)
1    return configs, attributes
1
1
1def _fetch_model_from_local(
1    uri: str, storage_path: str, model_file_type: ModelFileType
1):
1    """
1    Returns: TODO: Will be removed in future
1        configs: TConfigs
1        attributes: str
1    """
1    if model_file_type == ModelFileType.SAFETENSORS:
1        # copy anything in the uri to local_dir
1        for file in os.listdir(uri):
1            shutil.copy(os.path.join(uri, file), storage_path)
1        return _process_huggingface_files(storage_path)
1    # concat uri to get complete path
1    target_model_path = os.path.join(uri, MODEL_WEIGHTS_FILE_IN_PT)
1    model_storage_path = os.path.join(storage_path, MODEL_WEIGHTS_FILE_IN_PT)
1    target_config_path = os.path.join(uri, MODEL_CONFIG_FILE_IN_YAML)
1    config_storage_path = os.path.join(storage_path, MODEL_CONFIG_FILE_IN_YAML)
1
1    # check if file exist
1    exist_model_file = os.path.exists(target_model_path)
1    exist_config_file = os.path.exists(target_config_path)
1
1    configs = None
1    attributes = None
1    if exist_model_file and exist_config_file:
1        # copy config.yaml
1        shutil.copy(target_config_path, config_storage_path)
1        logger.info(
1            f"copy file from {target_config_path} to {config_storage_path} success"
1        )
1
1        # read and parse config dict from config.yaml
1        with open(config_storage_path, "r", encoding="utf-8") as file:
1            config_dict = yaml.safe_load(file)
1        configs, attributes = _parse_inference_config(config_dict)
1
1        # if config.yaml is correct, copy model file
1        shutil.copy(target_model_path, model_storage_path)
1        logger.info(
1            f"copy file from {target_model_path} to {model_storage_path} success"
1        )
1
1    elif not exist_model_file or not exist_config_file:
1        raise InvalidUriError(uri)
1
1    return configs, attributes
1
1
1def _parse_inference_config(config_dict):
1    """
1    Args:
1        config_dict: dict
1            - configs: dict
1                - input_shape (list<i32>): input shape of the model and needs to be two-dimensional array like [96, 2]
1                - output_shape (list<i32>): output shape of the model and needs to be two-dimensional array like [96, 2]
1                - input_type (list<str>): input type of the model and each element needs to be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text'], default float64
1                - output_type (list<str>): output type of the model and each element needs to be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text'], default float64
1            - attributes: dict
1    Returns:
1        configs: TConfigs
1        attributes: str
1    """
1    configs = config_dict["configs"]
1
1    # check if input_shape and output_shape are two-dimensional array
1    if not (
1        isinstance(configs["input_shape"], list) and len(configs["input_shape"]) == 2
1    ):
1        raise BadConfigValueError(
1            "input_shape",
1            configs["input_shape"],
1            "input_shape should be a two-dimensional array.",
1        )
1    if not (
1        isinstance(configs["output_shape"], list) and len(configs["output_shape"]) == 2
1    ):
1        raise BadConfigValueError(
1            "output_shape",
1            configs["output_shape"],
1            "output_shape should be a two-dimensional array.",
1        )
1
1    # check if input_shape and output_shape are positive integer
1    input_shape_is_positive_number = (
1        isinstance(configs["input_shape"][0], int)
1        and isinstance(configs["input_shape"][1], int)
1        and configs["input_shape"][0] > 0
1        and configs["input_shape"][1] > 0
1    )
1    if not input_shape_is_positive_number:
1        raise BadConfigValueError(
1            "input_shape",
1            configs["input_shape"],
1            "element in input_shape should be positive integer.",
1        )
1
1    output_shape_is_positive_number = (
1        isinstance(configs["output_shape"][0], int)
1        and isinstance(configs["output_shape"][1], int)
1        and configs["output_shape"][0] > 0
1        and configs["output_shape"][1] > 0
1    )
1    if not output_shape_is_positive_number:
1        raise BadConfigValueError(
1            "output_shape",
1            configs["output_shape"],
1            "element in output_shape should be positive integer.",
1        )
1
1    # check if input_type and output_type are one-dimensional array with right length
1    if "input_type" in configs and not (
1        isinstance(configs["input_type"], list)
1        and len(configs["input_type"]) == configs["input_shape"][1]
1    ):
1        raise BadConfigValueError(
1            "input_type",
1            configs["input_type"],
1            "input_type should be a one-dimensional array and length of it should be equal to input_shape[1].",
1        )
1
1    if "output_type" in configs and not (
1        isinstance(configs["output_type"], list)
1        and len(configs["output_type"]) == configs["output_shape"][1]
1    ):
1        raise BadConfigValueError(
1            "output_type",
1            configs["output_type"],
1            "output_type should be a one-dimensional array and length of it should be equal to output_shape[1].",
1        )
1
1    # parse input_type and output_type to byte
1    if "input_type" in configs:
1        input_type = [get_data_type_byte_from_str(x) for x in configs["input_type"]]
1    else:
1        input_type = [get_data_type_byte_from_str("float32")] * configs["input_shape"][
1            1
1        ]
1
1    if "output_type" in configs:
1        output_type = [get_data_type_byte_from_str(x) for x in configs["output_type"]]
1    else:
1        output_type = [get_data_type_byte_from_str("float32")] * configs[
1            "output_shape"
1        ][1]
1
1    # parse attributes
1    attributes = ""
1    if "attributes" in config_dict:
1        attributes = str(config_dict["attributes"])
1
1    return (
1        TConfigs(
1            configs["input_shape"], configs["output_shape"], input_type, output_type
1        ),
1        attributes,
1    )
1
1
1def _process_huggingface_files(local_dir: str):
1    """
1    TODO: Currently, we use this function to convert the model config from huggingface, we will refactor this in the future.
1    """
1    config_file = None
1    for config_name in ["config.json", "model_config.json"]:
1        config_path = os.path.join(local_dir, config_name)
1        if os.path.exists(config_path):
1            config_file = config_path
1            break
1
1    if not config_file:
1        raise InvalidUriError(f"No config.json found in {local_dir}")
1
1    safetensors_files = glob.glob(os.path.join(local_dir, "*.safetensors"))
1    if not safetensors_files:
1        raise InvalidUriError(f"No .safetensors files found in {local_dir}")
1
1    simple_config = {
1        "configs": {
1            "input_shape": [96, 1],
1            "output_shape": [96, 1],
1            "input_type": ["float32"],
1            "output_type": ["float32"],
1        },
1        "attributes": {
1            "model_type": "huggingface_model",
1            "source_dir": local_dir,
1            "files": [os.path.basename(f) for f in safetensors_files],
1        },
1    }
1
1    configs, attributes = _parse_inference_config(simple_config)
1    return configs, attributes
1