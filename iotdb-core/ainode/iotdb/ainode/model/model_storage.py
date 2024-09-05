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

import json
import os
import shutil
import threading
from typing import Dict

import torch
import torch._dynamo
import torch.nn as nn
from pylru import lrucache

from iotdb.ainode.config import AINodeDescriptor
from iotdb.ainode.constant import (OptionsKey, DEFAULT_MODEL_FILE_NAME,
                                   DEFAULT_CONFIG_FILE_NAME, ModelInputName)
from iotdb.ainode.exception import ModelNotExistError
from iotdb.ainode.log import Logger
from iotdb.ainode.model.model_factory import fetch_model_by_uri


class ModelStorage(object):
    _instance = None
    _first_init = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._first_init:
            self.__model_dir = os.path.join(os.getcwd(), AINodeDescriptor().get_config().get_ain_models_dir())
            if not os.path.exists(self.__model_dir):
                try:
                    os.makedirs(self.__model_dir)
                except PermissionError as e:
                    Logger().error(e)
                    raise e
            self.lock = threading.RLock()
            self.__model_cache = lrucache(AINodeDescriptor().get_config().get_ain_model_storage_cache_size())
            self._first_init = True

    def register_model(self, model_id: str, uri: str):
        """
        Args:
            model_id: id of model to register
            uri: network dir path or local dir path of model to register, where model.pt and config.yaml are required,
                e.g. https://huggingface.co/user/modelname/resolve/main/ or /Users/admin/Desktop/model
        Returns:
            configs: TConfigs
            attributes: str
        """
        storage_path = os.path.join(self.__model_dir, f'{model_id}')
        # create storage dir if not exist
        with self.lock:
            if not os.path.exists(storage_path):
                os.makedirs(storage_path)
        model_storage_path = os.path.join(storage_path, DEFAULT_MODEL_FILE_NAME)
        config_storage_path = os.path.join(storage_path, DEFAULT_CONFIG_FILE_NAME)
        return fetch_model_by_uri(uri, model_storage_path, config_storage_path)

    def save_model(self,
                   model: nn.Module,
                   model_config: Dict,
                   model_id: str,
                   trial_id: str) -> str:
        model_dir_path = os.path.join(self.__model_dir, f'{model_id}')
        with self.lock:
            if not os.path.exists(model_dir_path):
                os.makedirs(model_dir_path)
        model_file_path = os.path.join(model_dir_path, f'{trial_id}.pt')

        # Note: model config for time series should contain 'input_len' and 'input_vars'
        sample_input = (
            _pack_input_dict(
                torch.randn(1, model_config[OptionsKey.INPUT_LENGTH.name()], model_config[OptionsKey.INPUT_VARS.name()])
            )
        )
        with self.lock:
            torch.jit.save(torch.jit.trace(model, sample_input),
                           model_file_path,
                           _extra_files={'model_config': json.dumps(model_config)})
        return os.path.abspath(model_file_path)

    def load_model(self, model_id: str, acceleration: bool):
        """
        Returns:
            model: a ScriptModule contains model architecture and parameters, which can be deployed cross-platform
        """
        ain_models_dir = os.path.join(self.__model_dir, f'{model_id}')
        model_path = os.path.join(ain_models_dir, DEFAULT_MODEL_FILE_NAME)
        if model_path in self.__model_cache:
            model = self.__model_cache[model_path]
            if isinstance(model, torch._dynamo.eval_frame.OptimizedModule) or not acceleration:
                return model
            else:
                model = torch.compile(model)
                self.__model_cache[model_path] = model
                return model
        else:
            if not os.path.exists(model_path):
                raise ModelNotExistError(model_path)
            else:
                model = torch.jit.load(model_path)
                if acceleration:
                    try:
                        model = torch.compile(model)
                    except Exception as e:
                        Logger().warning(f"acceleration failed, fallback to normal mode: {str(e)}")
                self.__model_cache[model_path] = model
                return model

    def delete_model(self, model_id: str) -> None:
        """
        Args:
            model_id: id of model to delete
        Returns:
            None
        """
        storage_path = os.path.join(self.__model_dir, f'{model_id}')

        if os.path.exists(storage_path):
            for file_name in os.listdir(storage_path):
                self._remove_from_cache(os.path.join(storage_path, file_name))
            shutil.rmtree(storage_path)

    def _remove_from_cache(self, file_path: str) -> None:
        if file_path in self.__model_cache:
            del self.__model_cache[file_path]


def _pack_input_dict(batch_x: torch.Tensor,
                     batch_x_mark: torch.Tensor = None,
                     dec_inp: torch.Tensor = None,
                     batch_y_mark: torch.Tensor = None):
    """
    pack up inputs as a dict to adapt for different models
    """
    input_dict = {}
    if batch_x is not None:
        input_dict[ModelInputName.DATA_X.value] = batch_x
    if batch_x_mark is not None:
        input_dict[ModelInputName.TIME_STAMP_X] = batch_x_mark
    if dec_inp is not None:
        input_dict[ModelInputName.DEC_INP] = dec_inp
    if batch_y_mark is not None:
        input_dict[ModelInputName.TIME_STAMP_Y.value] = batch_y_mark
    return input_dict
