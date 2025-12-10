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

import importlib
import json
import os.path
import sys
from contextlib import contextmanager
from typing import Dict, Tuple

from iotdb.ainode.core.model.model_constants import (
    MODEL_CONFIG_FILE_IN_JSON,
    MODEL_WEIGHTS_FILE_IN_SAFETENSORS,
    UriType,
)


def parse_uri_type(uri: str) -> UriType:
    if uri.startswith("repo://"):
        return UriType.REPO
    elif uri.startswith("file://"):
        return UriType.FILE
    else:
        raise ValueError(
            f"Unsupported URI type: {uri}. Supported formats: repo:// or file://"
        )


def get_parsed_uri(uri: str) -> str:
    return uri[7:]  # Remove "repo://" or "file://" prefix


@contextmanager
def temporary_sys_path(path: str):
    """Context manager for temporarily adding a path to sys.path"""
    path_added = path not in sys.path
    if path_added:
        sys.path.insert(0, path)
    try:
        yield
    finally:
        if path_added and path in sys.path:
            sys.path.remove(path)


def load_model_config_in_json(config_path: str) -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_model_files(model_dir: str) -> Tuple[str, str]:
    """Validate model files exist, return config and weights file paths"""

    config_path = os.path.join(model_dir, MODEL_CONFIG_FILE_IN_JSON)
    weights_path = os.path.join(model_dir, MODEL_WEIGHTS_FILE_IN_SAFETENSORS)

    if not os.path.exists(config_path):
        raise ValueError(f"Model config file does not exist: {config_path}")
    if not os.path.exists(weights_path):
        raise ValueError(f"Model weights file does not exist: {weights_path}")

    # Create __init__.py file to ensure model directory can be imported as a module
    init_file = os.path.join(model_dir, "__init__.py")
    if not os.path.exists(init_file):
        with open(init_file, "w"):
            pass

    return config_path, weights_path


def import_class_from_path(module_name, class_path: str):
    file_name, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_name + "." + file_name)
    return getattr(module, class_name)


def ensure_init_file(dir_path: str):
    """Ensure __init__.py file exists in the given dir path"""
    init_file = os.path.join(dir_path, "__init__.py")
    os.makedirs(dir_path, exist_ok=True)
    if not os.path.exists(init_file):
        with open(init_file, "w"):
            pass
