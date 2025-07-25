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
from huggingface_hub import snapshot_download

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
from ainode.core.model.model_info import get_model_loading_strategy

HTTP_PREFIX = "http://"
HTTPS_PREFIX = "https://"

logger = Logger()


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
    """
    """
    is_network_path, parsed_uri = _parse_uri(uri)
    strategy = get_model_loading_strategy(uri)
    
    if strategy == "network_huggingface":
        return _register_huggingface_model_from_network(parsed_uri, model_storage_path, config_storage_path)
    elif strategy == "local_huggingface":
        return _register_huggingface_model_from_local(parsed_uri, model_storage_path, config_storage_path)
    elif strategy == "local_pytorch":
        return _register_model_from_local(parsed_uri, model_storage_path, config_storage_path)
    elif is_network_path:
        return _register_model_from_network(parsed_uri, model_storage_path, config_storage_path)
    else:
        return _register_model_from_local(parsed_uri, model_storage_path, config_storage_path)
    
def _register_huggingface_model_from_network(repo_id: str, model_storage_path: str, config_storage_path: str):
    import tempfile
    
    temp_dir = tempfile.mkdtemp(prefix="hf_model_")
    
    try:
        snapshot_download(
            repo_id=repo_id,
            local_dir=temp_dir,
            local_dir_use_symlinks=False,
        )
        
        return _process_huggingface_files(temp_dir, model_storage_path, config_storage_path)
        
    except Exception as e:
        logger.error(f"Failed to download HuggingFace model {repo_id}: {e}")
        raise InvalidUriError(repo_id)

def _register_huggingface_model_from_local(local_path: str, model_storage_path: str, config_storage_path: str):
    return _process_huggingface_files(local_path, model_storage_path, config_storage_path)

def _process_huggingface_files(source_dir: str, model_storage_path: str, config_storage_path: str):
    import glob
    import json
    
    config_file = None
    for config_name in ["config.json", "model_config.json"]:
        config_path = os.path.join(source_dir, config_name)
        if os.path.exists(config_path):
            config_file = config_path
            break
    
    if not config_file:
        raise InvalidUriError(f"No config.json found in {source_dir}")
    
    safetensors_files = glob.glob(os.path.join(source_dir, "*.safetensors"))
    if not safetensors_files:
        raise InvalidUriError(f"No .safetensors files found in {source_dir}")
    
    with open(config_file, "r", encoding="utf-8") as f:
        hf_config = json.load(f)
    
    ainode_config = _convert_hf_config_to_ainode(hf_config, source_dir)
    
    with open(config_storage_path, "w", encoding="utf-8") as f:
        yaml.dump(ainode_config, f)
    
    with open(model_storage_path, "w") as f:
        f.write(f"# HuggingFace model from: {source_dir}\n")
        f.write(f"# Model files: {[os.path.basename(f) for f in safetensors_files]}\n")
        f.write(f"# Source directory: {source_dir}\n")
    
    configs, attributes = _parse_inference_config(ainode_config)
    return configs, attributes

def _convert_hf_config_to_ainode(hf_config: dict, source_dir: str) -> dict:
    input_length = 96  
    output_length = 96  
    
    if "max_position_embeddings" in hf_config:
        input_length = min(hf_config["max_position_embeddings"], 512)
    if "prediction_length" in hf_config:
        output_length = hf_config["prediction_length"]
    
    ainode_config = {
        "configs": {
            "input_shape": [input_length, 1],
            "output_shape": [output_length, 1],
            "input_type": ["float32"],
            "output_type": ["float32"]
        },
        "attributes": {
            "model_type": "huggingface_transformers",
            "source_dir": source_dir,
            "hf_config": hf_config,
            "predict_length": output_length
        }
    }
    
    return ainode_config