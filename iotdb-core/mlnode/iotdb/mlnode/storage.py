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
from typing import Dict, Tuple

import torch
import torch.nn as nn
from pylru import lrucache

from iotdb.mlnode.config import descriptor
from iotdb.mlnode.exception import ModelNotExistError
from iotdb.mlnode.log import logger


class ModelStorage(object):
    def __init__(self):
        self.__model_dir = os.path.join('.', descriptor.get_config().get_mn_model_storage_dir())
        if not os.path.exists(self.__model_dir):
            try:
                os.mkdir(self.__model_dir)
            except PermissionError as e:
                logger.error(e)
                raise e
        self.lock = threading.RLock()
        self.__model_cache = lrucache(descriptor.get_config().get_mn_model_storage_cache_size())

    def save_model(self,
                   model: nn.Module,
                   model_config: Dict,
                   model_id: str,
                   trial_id: str) -> str:
        model_dir_path = os.path.join(self.__model_dir, f'{model_id}')
        if not os.path.exists(model_dir_path):
            os.makedirs(model_dir_path)
        model_file_path = os.path.join(model_dir_path, f'{trial_id}.pt')

        # Note: model config for time series should contain 'input_len' and 'input_vars'
        sample_input = [torch.randn(1, model_config['input_len'], model_config['input_vars'])]
        self.lock.acquire()
        torch.jit.save(torch.jit.trace(model, sample_input),
                       model_file_path,
                       _extra_files={'model_config': json.dumps(model_config)})
        self.lock.release()
        return os.path.abspath(model_file_path)

    def load_model(self, file_path: str) -> Tuple[torch.jit.ScriptModule, Dict]:
        """
        Returns:
            jit_model: a ScriptModule contains model architecture and parameters, which can be deployed cross-platform
            model_config: a dict contains model attributes
        """
        file_path = os.path.join(self.__model_dir, file_path)
        if file_path in self.__model_cache:
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

    def delete_model(self, model_id: str) -> None:
        model_dir_path = os.path.join(self.__model_dir, f'{model_id}')
        if os.path.exists(model_dir_path):
            for file_name in os.listdir(model_dir_path):
                self.__remove_from_cache(os.path.join(model_dir_path, file_name))
            shutil.rmtree(model_dir_path)

    def delete_trial(self, model_id: str, trial_id: str) -> None:
        model_file_path = os.path.join(self.__model_dir, f'{model_id}', f'{trial_id}.pt')
        self.__remove_from_cache(model_file_path)
        if os.path.exists(model_file_path):
            os.remove(model_file_path)

    def __remove_from_cache(self, file_path: str) -> None:
        if file_path in self.__model_cache:
            del self.__model_cache[file_path]


model_storage = ModelStorage()
