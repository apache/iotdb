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
from pathlib import Path
from typing import Any

import torch
from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    AutoModelForNextSentencePrediction,
    AutoModelForSeq2SeqLM,
    AutoModelForSequenceClassification,
    AutoModelForTimeSeriesPrediction,
    AutoModelForTokenClassification,
)

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.exception import ModelNotExistException
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.device_manager import DeviceManager
from iotdb.ainode.core.model.model_constants import ModelCategory
from iotdb.ainode.core.model.model_info import ModelInfo
from iotdb.ainode.core.model.sktime.modeling_sktime import create_sktime_model
from iotdb.ainode.core.model.utils import import_class_from_path, temporary_sys_path

logger = Logger()
BACKEND = DeviceManager()


def load_model(model_info: ModelInfo, **model_kwargs) -> Any:
    if model_info.auto_map is not None:
        model = load_model_from_transformers(model_info, **model_kwargs)
    else:
        if model_info.model_type == "sktime":
            model = create_sktime_model(model_info.model_id)
        else:
            model = load_model_from_pt(model_info, **model_kwargs)

    logger.info(
        f"Model {model_info.model_id} loaded to device {model.device if model_info.model_type != 'sktime' else 'cpu'} successfully."
    )
    return model


def load_model_from_transformers(model_info: ModelInfo, **model_kwargs):
    device_map = model_kwargs.get("device_map", "cpu")
    trust_remote_code = model_kwargs.get("trust_remote_code", True)
    train_from_scratch = model_kwargs.get("train_from_scratch", False)

    model_path = os.path.join(
        os.getcwd(),
        AINodeDescriptor().get_config().get_ain_models_dir(),
        model_info.category.value,
        model_info.model_id,
    )

    config_str = model_info.auto_map.get("AutoConfig", "")
    model_str = model_info.auto_map.get("AutoModelForCausalLM", "")

    if model_info.category == ModelCategory.BUILTIN:
        module_name = (
            AINodeDescriptor().get_config().get_ain_models_builtin_dir()
            + "."
            + model_info.model_id
        )
        config_cls = import_class_from_path(module_name, config_str)
        model_cls = import_class_from_path(module_name, model_str)
    elif model_str and config_str:
        module_parent = str(Path(model_path).parent.absolute())
        with temporary_sys_path(module_parent):
            config_cls = import_class_from_path(model_info.model_id, config_str)
            model_cls = import_class_from_path(model_info.model_id, model_str)
    else:
        config_cls = AutoConfig.from_pretrained(model_path)
        if type(config_cls) in AutoModelForTimeSeriesPrediction._model_mapping.keys():
            model_cls = AutoModelForTimeSeriesPrediction
        elif (
            type(config_cls) in AutoModelForNextSentencePrediction._model_mapping.keys()
        ):
            model_cls = AutoModelForNextSentencePrediction
        elif type(config_cls) in AutoModelForSeq2SeqLM._model_mapping.keys():
            model_cls = AutoModelForSeq2SeqLM
        elif (
            type(config_cls) in AutoModelForSequenceClassification._model_mapping.keys()
        ):
            model_cls = AutoModelForSequenceClassification
        elif type(config_cls) in AutoModelForTokenClassification._model_mapping.keys():
            model_cls = AutoModelForTokenClassification
        else:
            model_cls = AutoModelForCausalLM

    if train_from_scratch:
        model = model_cls.from_config(config_cls, trust_remote_code=trust_remote_code)
    else:
        model = model_cls.from_pretrained(
            model_path, trust_remote_code=trust_remote_code
        )

    return BACKEND.move_model(model, device_map)


def load_model_from_pt(model_info: ModelInfo, **kwargs):
    device_map = kwargs.get("device_map", "cpu")
    acceleration = kwargs.get("acceleration", False)
    model_path = os.path.join(
        os.getcwd(),
        AINodeDescriptor().get_config().get_ain_models_dir(),
        model_info.category.value,
        model_info.model_id,
    )
    model_file = os.path.join(model_path, "model.pt")
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


def load_model_for_efficient_inference():
    # TODO: An efficient model loading method for inference based on model_arguments
    pass


def load_model_for_powerful_finetune():
    # TODO: An powerful model loading method for finetune based on model_arguments
    pass


def unload_model():
    pass
