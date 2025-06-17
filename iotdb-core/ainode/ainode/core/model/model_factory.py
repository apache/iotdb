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
import json
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
    IOTDB_CONFIG_FILES,
    IOTDB_WEIGHT_FILES,
    MODEL_FORMAT_PRIORITY,
    WEIGHT_FORMAT_PRIORITY,
)
from ainode.core.exception import BadConfigValueError, InvalidUriError
from ainode.core.log import Logger
from ainode.core.util.serde import get_data_type_byte_from_str
from ainode.thrift.ainode.ttypes import TConfigs

from ainode.core.model.config_parser import (
    parse_config_file,
    convert_iotdb_config_to_ainode_format,
    validate_iotdb_config
)
from ainode.core.model.safetensor_loader import load_weights_as_state_dict

HTTP_PREFIX = "http://"
HTTPS_PREFIX = "https://"

logger = Logger()

def _detect_model_format(base_path: str) -> tuple:
    """
    检测模型格式，支持IoTDB和legacy格式
    
    Args:
        base_path: 模型目录路径或网络URI
        
    Returns:
        (format_type, config_file, weight_file): 格式类型和对应的文件名
    """
    base_path = Path(base_path) if not base_path.startswith(('http://', 'https://')) else base_path
    
    # 首先检查IoTDB格式 (优先级更高)
    for config_file in IOTDB_CONFIG_FILES:
        if isinstance(base_path, Path):
            config_path = base_path / config_file
            if config_path.exists():
                # 查找对应的权重文件
                for weight_file in WEIGHT_FORMAT_PRIORITY:
                    weight_path = base_path / weight_file
                    if weight_path.exists():
                        logger.info(f"检测到IoTDB格式: {config_file} + {weight_file}")
                        return "iotdb", config_file, weight_file
        else:
            # 网络路径暂时跳过检测，在下载时处理
            pass
    
    # 检查legacy格式
    if isinstance(base_path, Path):
        legacy_config = base_path / DEFAULT_CONFIG_FILE_NAME
        legacy_model = base_path / DEFAULT_MODEL_FILE_NAME
        if legacy_config.exists() and legacy_model.exists():
            logger.info("检测到legacy格式")
            return "legacy", DEFAULT_CONFIG_FILE_NAME, DEFAULT_MODEL_FILE_NAME
    
    return None, None, None


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
    
def _download_file_with_fallback(base_uri: str, file_candidates: list, storage_path: str) -> str:
    """
    按优先级顺序尝试下载文件
    
    Args:
        base_uri: 基础URI
        file_candidates: 候选文件名列表
        storage_path: 存储路径
        
    Returns:
        成功下载的文件名
    """
    base_uri = base_uri if base_uri.endswith("/") else base_uri + "/"
    
    for filename in file_candidates:
        try:
            file_url = urljoin(base_uri, filename)
            _download_file(file_url, storage_path)
            logger.info(f"成功下载文件: {filename}")
            return filename
        except Exception as e:
            logger.debug(f"下载文件 {filename} 失败: {e}")
            continue
    
    raise InvalidUriError(f"无法从 {base_uri} 下载任何候选文件: {file_candidates}")


def _register_model_from_network(
    uri: str, model_storage_path: str, config_storage_path: str
) -> [TConfigs, str]:
    """
    从网络注册模型，完全集成 config_parser 和 safetensor_loader
    """
    uri = uri if uri.endswith("/") else uri + "/"
    
    # 尝试下载配置文件（IoTDB格式优先）
    try:
        config_filename = _download_file_with_fallback(
            uri, IOTDB_CONFIG_FILES + [DEFAULT_CONFIG_FILE_NAME], config_storage_path
        )
        format_type = "iotdb" if config_filename in IOTDB_CONFIG_FILES else "legacy"
    except Exception as e:
        logger.error(f"配置文件下载失败: {e}")
        raise InvalidUriError(uri)
    
    # 使用 config_parser 解析配置文件
    try:
        config_dict = parse_config_file(config_storage_path)
        
        if format_type == "iotdb":
            # 验证IoTDB配置
            if not validate_iotdb_config(config_dict):
                raise BadConfigValueError("config_file", config_storage_path, "IoTDB配置验证失败")
            
            # 转换IoTDB配置为AINode格式
            ainode_config = convert_iotdb_config_to_ainode_format(config_dict)
            configs, attributes = _parse_inference_config(ainode_config)
        else:
            # 处理legacy格式
            configs, attributes = _parse_inference_config(config_dict)
    except Exception as e:
        logger.error(f"配置文件解析失败: {e}")
        raise BadConfigValueError("config_file", config_storage_path, str(e))
    
    # 下载模型权重文件
    try:
        weight_candidates = WEIGHT_FORMAT_PRIORITY if format_type == "iotdb" else [DEFAULT_MODEL_FILE_NAME]
        weight_filename = _download_file_with_fallback(uri, weight_candidates, model_storage_path)
        
        # 使用 safetensor_loader 验证下载的权重文件
        if format_type == "iotdb":
            try:
                weights = load_weights_as_state_dict(model_storage_path)
                logger.info(f"权重文件验证成功，包含 {len(weights)} 个参数")
            except Exception as e:
                logger.error(f"下载的权重文件验证失败: {e}")
                raise InvalidUriError(f"权重文件损坏: {weight_filename}")
                
    except Exception as e:
        logger.error(f"模型文件下载失败: {e}")
        raise InvalidUriError(uri)
    
    return configs, attributes


# 修改 _register_model_from_local 函数
def _register_model_from_local(
    uri: str, model_storage_path: str, config_storage_path: str
) -> [TConfigs, str]:
    """
    从本地注册模型，完全集成 config_parser 和 safetensor_loader
    """
    # 检测模型格式
    format_type, config_file, weight_file = _detect_model_format(uri)
    
    if format_type is None:
        raise InvalidUriError(f"未找到有效的模型文件在路径: {uri}")
    
    source_config_path = os.path.join(uri, config_file)
    source_model_path = os.path.join(uri, weight_file)
    
    # 复制配置文件
    logger.debug(f"复制配置文件: {source_config_path} -> {config_storage_path}")
    shutil.copy(source_config_path, config_storage_path)
    
    # 使用 config_parser 解析配置文件
    try:
        config_dict = parse_config_file(config_storage_path)
        
        if format_type == "iotdb":
            # 验证IoTDB配置
            if not validate_iotdb_config(config_dict):
                raise BadConfigValueError("config_file", config_storage_path, "IoTDB配置验证失败")
            
            # 验证源权重文件
            try:
                weights = load_weights_as_state_dict(source_model_path)
                logger.info(f"源权重文件验证成功，包含 {len(weights)} 个参数")
            except Exception as e:
                logger.error(f"源权重文件验证失败: {e}")
                raise InvalidUriError(f"权重文件损坏: {source_model_path}")
            
            ainode_config = convert_iotdb_config_to_ainode_format(config_dict)
            configs, attributes = _parse_inference_config(ainode_config)
        else:
            configs, attributes = _parse_inference_config(config_dict)
            
    except Exception as e:
        logger.error(f"配置文件解析失败: {e}")
        raise BadConfigValueError("config_file", config_storage_path, str(e))
    
    # 复制模型文件
    logger.debug(f"复制模型文件: {source_model_path} -> {model_storage_path}")
    shutil.copy(source_model_path, model_storage_path)
    
    return configs, attributes
def _register_model_from_local_old(
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
    # concat uri to get complete path
    target_model_path = os.path.join(uri, DEFAULT_MODEL_FILE_NAME)
    target_config_path = os.path.join(uri, DEFAULT_CONFIG_FILE_NAME)

    # check if file exist
    exist_model_file = os.path.exists(target_model_path)
    exist_config_file = os.path.exists(target_config_path)

    configs = None
    attributes = None
    if exist_model_file and exist_config_file:
        # copy config.yaml
        logger.debug(f"copy file from {target_config_path} to {config_storage_path}")
        shutil.copy(target_config_path, config_storage_path)
        logger.debug(
            f"copy file from {target_config_path} to {config_storage_path} success"
        )

        # read and parse config dict from config.yaml
        with open(config_storage_path, "r", encoding="utf-8") as file:
            config_dict = yaml.safe_load(file)
        configs, attributes = _parse_inference_config(config_dict)

        # if config.yaml is correct, copy model file
        logger.debug(f"copy file from {target_model_path} to {model_storage_path}")
        shutil.copy(target_model_path, model_storage_path)
        logger.debug(
            f"copy file from {target_model_path} to {model_storage_path} success"
        )

    elif not exist_model_file or not exist_config_file:
        raise InvalidUriError(uri)

    return configs, attributes

def _convert_iotdb_config_to_ainode_format(iotdb_config: dict) -> dict:
    """
    将IoTDB配置转换为AINode格式
    """
    model_type = iotdb_config.get("model_type", "unknown")
    input_length = iotdb_config.get("input_token_len", 96)
    output_length = iotdb_config.get("output_token_lens", [96])[0]
    
    ainode_config = {
        "configs": {
            "input_shape": [input_length, 1],
            "output_shape": [output_length, 1],
            "input_type": ["float32"],
            "output_type": ["float32"],
        },
        "attributes": {
            "model_type": model_type,
            "iotdb_model": True,
            "original_config": iotdb_config,
        },
    }
    
    logger.debug(f"转换IoTDB配置: {model_type} -> AINode格式")
    return ainode_config


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
