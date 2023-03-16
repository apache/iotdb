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
import json
import torch
import shutil
import torch.nn as nn
from pylru import lrucache
from iotdb.mlnode.exception import ModelNotExistError
from iotdb.mlnode.config import config


# TODO: Add permission check firstly
# TODO: Consider concurrency, maybe
class ModelStorage(object):
    def __init__(self,
                 root_path: str,
                 cache_size: int):
        self.__model_dir = os.path.join(os.getcwd(), root_path)
        if not os.path.exists(self.__model_dir):
            os.mkdir(self.__model_dir)
        self.__model_cache = lrucache(cache_size)

    def save_model(self,
                   model: nn.Module,
                   model_config: dict,
                   model_id: str,
                   trial_id: str) -> bool:
        """
        Return: True if successfully saved

        Note: model config for time series should contain 'input_len' and 'input_vars'
        """
        fold_path = os.path.join(self.__model_dir, f'{model_id}')
        if not os.path.exists(fold_path):
            os.mkdir(fold_path)
        sample_input = [torch.randn(1, model_config['input_len'], model_config['input_vars'])]
        try:
            torch.jit.save(torch.jit.trace(model, sample_input),
                           os.path.join(fold_path, f'{trial_id}.pt'),
                           _extra_files={'model_config': json.dumps(model_config)})
        except PermissionError:
            return False
        return True

    def load_model(self, model_id: str, trial_id: str) -> (torch.jit.ScriptModule, dict):
        """
        Return:
            jit_model: a ScriptModule contains model architecture and parameters, which can be deployed cross-platform
            model_config: a dict contains model attributes
        """
        file_path = os.path.join(self.__model_dir, f'{model_id}', f'{trial_id}.pt')
        if model_id in self.__model_cache:
            return self.__model_cache[file_path]
        else:
            if not os.path.exists(file_path):
                raise ModelNotExistError(file_path)
            else:
                tmp_dict = {'model_config': ''}
                jit_model = torch.jit.load(file_path, _extra_files=tmp_dict)
                model_config = json.loads(tmp_dict['model_config'])
                self.__model_cache[file_path] = jit_model, model_config
                return jit_model, model_config

    def _remove_from_cache(self, key: str):
        if key in self.__model_cache:
            del self.__model_cache[key]

    def delete_trial(self, model_id: str, trial_id: str) -> bool:
        """
        Return: True if successfully deleted
        """
        file_path = os.path.join(self.__model_dir, f'{model_id}', f'{trial_id}.pt')
        self._remove_from_cache(file_path)
        if os.path.exists(file_path):
            os.remove(file_path)
        return not os.path.exists(file_path)

    def delete_model(self, model_id: str) -> bool:
        """
        Return: True if successfully deleted
        """
        folder_path = os.path.join(self.__model_dir, f'{model_id}')
        if os.path.exists(folder_path):
            for file_name in os.listdir(folder_path):
                self._remove_from_cache(os.path.join(folder_path, file_name))
            shutil.rmtree(folder_path)
        return not os.path.exists(folder_path)

    def send_model(self):  # TODO: inference on db in future
        pass


# initialize a singleton
model_storage = ModelStorage(root_path=config.get_mn_model_storage_dir(),
                             cache_size=config.get_mn_model_storage_cachesize())
