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
import os
import shutil
from urllib.parse import urljoin, urlparse
from pathlib import Path

import yaml
from requests import Session
from requests.adapters import HTTPAdapter

from ainode.core.constant import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_CONFIG_FILE_NAME,
    DEFAULT_MODEL_FILE_NAME,
    DEFAULT_RECONNECT_TIMEOUT,
    DEFAULT_RECONNECT_TIMES,
)
from ainode.core.exception import BadConfigValueError, InvalidUriError
from ainode.core.log import Logger
from ainode.core.util.serde import get_data_type_byte_from_str
from ainode.thrift.ainode.ttypes import TConfigs
from ainode.core.model.config_parser import parse_config_file, convert_iotdb_config_to_ainode_format

HTTP_PREFIX = "http://"
HTTPS_PREFIX = "https://"

logger = Logger()

# IoTDB 模型相关文件名
IOTDB_CONFIG_FILES = ["config.json", "configuration.json"]
IOTDB_WEIGHT_FILES = ["model.safetensors", "pytorch_model.safetensors", "model.pt", "pytorch_model.pt"]


def _parse_uri(uri):
    """
    Args:
        uri (str): uri to parse
    Returns:
        is_network_path (bool): True if the url is a network path, False otherwise
        parsed_uri (str): parsed uri to get related file
    """

    parse_result = urlparse(uri)
    is_network_path = parse_result.scheme in ("http", "https")
    if is_network_path:
        return True, uri

    # handle file:// in uri
    if parse_result.scheme == "file":
        uri = uri[7:]

    # handle ~ in uri
    uri = os.path.expanduser(uri)
    return False, uri


def _download_file(url: str, storage_path: str) -> None:
    """
    Args:
        url: url of file to download
        storage_path: path to save the file
    Returns:
        None
    """
    logger.debug(f"download file from {url} to {storage_path}")

    session = Session()
    adapter = HTTPAdapter(max_retries=DEFAULT_RECONNECT_TIMES)
    session.mount(HTTP_PREFIX, adapter)
    session.mount(HTTPS_PREFIX, adapter)

    response = session.get(url, timeout=DEFAULT_RECONNECT_TIMEOUT, stream=True)
    response.raise_for_status()

    with open(storage_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
            if chunk:
                file.write(chunk)

    logger.debug(f"download file from {url} to {storage_path} success")


def _detect_model_format(base_path: str) -> tuple:
    """
    检测模型格式：legacy (model.pt + config.yaml) 或 IoTDB (config.json + safetensors)
    
    Args:
        base_path: 模型目录路径
        
    Returns:
        (format_type, config_file, weight_file): 格式类型和对应的文件名
    """
    base_path = Path(base_path)
    
    # 检查 IoTDB 格式
    for config_file in IOTDB_CONFIG_FILES:
        config_path = base_path / config_file
        if config_path.exists():
            # 查找权重文件
            for weight_file in IOTDB_WEIGHT_FILES:
                weight_path = base_path / weight_file
                if weight_path.exists():
                    logger.debug(f"检测到 IoTDB 格式: {config_file} + {weight_file}")
                    return "iotdb", config_file, weight_file
    
    # 检查 legacy 格式
    legacy_config = base_path / DEFAULT_CONFIG_FILE_NAME
    legacy_model = base_path / DEFAULT_MODEL_FILE_NAME
    if legacy_config.exists() and legacy_model.exists():
        logger.debug(f"检测到 legacy 格式: {DEFAULT_CONFIG_FILE_NAME} + {DEFAULT_MODEL_FILE_NAME}")
        return "legacy", DEFAULT_CONFIG_FILE_NAME, DEFAULT_MODEL_FILE_NAME
    
    return None, None, None


def _register_model_from_network(
    uri: str, model_storage_path: str, config_storage_path: str
) -> [TConfigs, str]:
    """
    Args:
        uri: network dir path of model to register, where model.pt and config.yaml are required,
            e.g. https://huggingface.co/user/modelname/resolve/main/
        model_storage_path: path to save model.pt
        config_storage_path: path to save config.yaml
    Returns:
        configs: TConfigs
        attributes: str
    """
    # concat uri to get complete url
    uri = uri if uri.endswith("/") else uri + "/"
    
    # 首先尝试检测 IoTDB 格式
    iotdb_detected = False
    configs, attributes = None, None
    
    for config_file in IOTDB_CONFIG_FILES:
        try:
            target_config_path = urljoin(uri, config_file)
            _download_file(target_config_path, config_storage_path)
            
            # 解析 IoTDB 配置
            iotdb_config = parse_config_file(config_storage_path)
            ainode_config = convert_iotdb_config_to_ainode_format(iotdb_config)
            configs, attributes = _parse_inference_config(ainode_config)
            
            # 查找对应的权重文件
            for weight_file in IOTDB_WEIGHT_FILES:
                try:
                    target_model_path = urljoin(uri, weight_file)
                    _download_file(target_model_path, model_storage_path)
                    iotdb_detected = True
                    logger.info(f"成功下载 IoTDB 模型: {config_file} + {weight_file}")
                    break
                except Exception as e:
                    logger.debug(f"未找到权重文件 {weight_file}: {e}")
                    continue
            
            if iotdb_detected:
                break
                
        except Exception as e:
            logger.debug(f"未找到配置文件 {config_file}: {e}")
            continue
    
    # 如果未检测到 IoTDB 格式，尝试 legacy 格式
    if not iotdb_detected:
        logger.debug("未检测到 IoTDB 格式，尝试 legacy 格式")
        target_model_path = urljoin(uri, DEFAULT_MODEL_FILE_NAME)
        target_config_path = urljoin(uri, DEFAULT_CONFIG_FILE_NAME)

        # download config file
        _download_file(target_config_path, config_storage_path)

        # read and parse config dict from config.yaml
        with open(config_storage_path, "r", encoding="utf-8") as file:
            config_dict = yaml.safe_load(file)
        configs, attributes = _parse_inference_config(config_dict)

        # if config.yaml is correct, download model file
        _download_file(target_model_path, model_storage_path)
        
    return configs, attributes


def _register_model_from_local(
    uri: str, model_storage_path: str, config_storage_path: str
) -> [TConfigs, str]:
    """
    Args:
        uri: local dir path of model to register, where model.pt and config.yaml are required,
            e.g. /Users/admin/Desktop/model
        model_storage_path: path to save model.pt
        config_storage_path: path to save config.yaml
    Returns:
        configs: TConfigs
        attributes: str
    """
    # 检测模型格式
    format_type, config_file, weight_file = _detect_model_format(uri)
    
    if format_type is None:
        raise InvalidUriError(f"未找到有效的模型文件在路径: {uri}")
    
    target_config_path = os.path.join(uri, config_file)
    target_model_path = os.path.join(uri, weight_file)
    
    # 复制配置文件
    logger.debug(f"copy file from {target_config_path} to {config_storage_path}")
    shutil.copy(target_config_path, config_storage_path)
    logger.debug(f"copy file from {target_config_path} to {config_storage_path} success")
    
    # 解析配置文件
    if format_type == "iotdb":
        # IoTDB 格式
        iotdb_config = parse_config_file(config_storage_path)
        ainode_config = convert_iotdb_config_to_ainode_format(iotdb_config)
        configs, attributes = _parse_inference_config(ainode_config)
    else:
        # legacy 格式
        with open(config_storage_path, "r", encoding="utf-8") as file:
            config_dict = yaml.safe_load(file)
        configs, attributes = _parse_inference_config(config_dict)
    
    # 复制模型文件
    logger.debug(f"copy file from {target_model_path} to {model_storage_path}")
    shutil.copy(target_model_path, model_storage_path)
    logger.debug(f"copy file from {target_model_path} to {model_storage_path} success")
    
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


def fetch_model_by_uri(uri: str, model_storage_path: str, config_storage_path: str):
    is_network_path, uri = _parse_uri(uri)

    if is_network_path:
        return _register_model_from_network(
            uri, model_storage_path, config_storage_path
        )
    else:
        return _register_model_from_local(uri, model_storage_path, config_storage_path)