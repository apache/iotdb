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
from typing import Any

import torch

from iotdb.ainode.core.exception import ModelNotExistException
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.device_manager import DeviceManager
from iotdb.ainode.core.model.model_constants import MODEL_PT
from iotdb.ainode.core.model.model_info import ModelInfo
from iotdb.ainode.core.model.sktime.modeling_sktime import create_sktime_model
from iotdb.ainode.core.model.utils import (
    get_model_and_config_by_auto_class,
    get_model_and_config_by_native_code,
    get_model_path,
)

logger = Logger()
BACKEND = DeviceManager()


def load_model(model_info: ModelInfo, **model_kwargs) -> Any:
    if model_info.auto_map is not None:
        model = load_transformers_model(model_info, **model_kwargs)
    elif model_info.hub_mixin_cls is not None:
        model = _load_hub_mixin_model(model_info, **model_kwargs)
    else:
        if model_info.model_type == "sktime":
            model = create_sktime_model(model_info.model_id)
        else:
            model = _load_torchscript_model(model_info, **model_kwargs)

    logger.info(
        f"Model {model_info.model_id} loaded to device {next(model.parameters()).device if model_info.model_type != 'sktime' else 'cpu'} successfully."
    )
    return model


def load_transformers_model(model_info: ModelInfo, **model_kwargs):
    device_map = model_kwargs.get("device_map", "cpu")
    trust_remote_code = model_kwargs.get("trust_remote_code", True)
    train_from_scratch = model_kwargs.get("train_from_scratch", False)

    model_path = get_model_path(model_info)

    model_class, config_instance = get_model_and_config_by_native_code(model_info)
    if model_class is None:
        model_class, config_instance = get_model_and_config_by_auto_class(model_path)

    # ---- Load base model ----
    if train_from_scratch:
        model = model_class.from_config(
            config_instance, trust_remote_code=trust_remote_code
        )
    else:
        model = model_class.from_pretrained(
            model_path,
            config=config_instance,
            trust_remote_code=trust_remote_code,
        )

    return BACKEND.move_model(model, device_map)


def _load_hub_mixin_model(model_info: ModelInfo, **model_kwargs):
    device_map = model_kwargs.get("device_map", "cpu")
    model_path = get_model_path(model_info)
    model_class, _ = get_model_and_config_by_native_code(model_info)
    if model_class is None:
        logger.error(f"Model class not found for '{model_info.model_id}'")
        raise ModelNotExistException(model_info.model_id)
    # Load model
    model = model_class.from_pretrained(model_path)
    return BACKEND.move_model(model, device_map)


def _load_torchscript_model(model_info: ModelInfo, **kwargs):
    device_map = kwargs.get("device_map", "cpu")
    acceleration = kwargs.get("acceleration", False)
    model_path = get_model_path(model_info)
    model_file = os.path.join(model_path, MODEL_PT)
    if not os.path.exists(model_file):
        logger.error(f"Model file not found at {model_file}.")
        raise ModelNotExistException(model_file)
    model = torch.jit.load(model_file)
    if isinstance(model, torch._dynamo.eval_frame.OptimizedModule) or not acceleration:
        return model
    try:
        model = torch.compile(model)
    except Exception as e:
        logger.warning(f"acceleration failed, fallback to normal mode: {str(e)}")
    return BACKEND.move_model(model, device_map)
