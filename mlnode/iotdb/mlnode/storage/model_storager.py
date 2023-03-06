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
from pylru import lrucache
from iotdb.mlnode.constant import (MLNODE_MODEL_STORAGE_DIR,
                                   MLNODE_MODEL_STORAGE_CACHESIZE)


# TODO: Concurrency
class ModelStorager(object):
    def __init__(self, root_path='ml_models', cache_size=30):
        self.root_path = root_path
        if not os.path.exists(root_path):
            os.mkdir(root_path)
        self._loaded_model_cache = lrucache(cache_size)

    def save_model(self, model, model_config, model_id, trial_id):
        """
        Return: True if successfully saved
        """
        fold_path = f'{self.root_path}/mid_{model_id}/'
        if not os.path.exists(fold_path):
            os.mkdir(fold_path)
        torch.jit.save(torch.jit.script(model),
                       f'{fold_path}/tid_{trial_id}.pt',
                       _extra_files={'model_config': json.dumps(model_config)})
        return os.path.exists(f'{fold_path}/tid_{trial_id}.pt')

    def load_model(self, model_id, trial_id):
        file_path = f'{self.root_path}/mid_{model_id}/tid_{trial_id}.pt'
        if model_id in self._loaded_model_cache:
            return self._loaded_model_cache[file_path]
        else:
            if not os.path.exists(file_path):
                raise RuntimeError('Model path (%s) is not found' % file_path)
            else:
                tmp_dict = {'model_config': ''}
                jit_model = torch.jit.load(file_path, _extra_files=tmp_dict)
                model_config = json.loads(tmp_dict['model_config'])
                self._loaded_model_cache[file_path] = jit_model, model_config
                return jit_model, model_config

    def _remove_from_cache(self, key):
        if key in self._loaded_model_cache:
            del self._loaded_model_cache[key]

    def delete_trial(self, model_id, trial_id):
        """
        Return: True if successfully deleted
        """
        file_path = f'{self.root_path}/mid_{model_id}/tid_{trial_id}.pt'
        self._remove_from_cache(file_path)
        if os.path.exists(file_path):
            os.remove(file_path)
        return not os.path.exists(file_path)

    def delete_model(self, model_id):
        """
        Return: True if successfully deleted
        """
        folder_path = f'{self.root_path}/mid_{model_id}/'
        if os.path.exists(folder_path):
            for file_name in os.listdir(folder_path):
                self._remove_from_cache(f'{folder_path}/{file_name}')
            shutil.rmtree(folder_path)
        return not os.path.exists(folder_path)

    def send_model(self):  # TODO: inference on db in future
        pass


modelStorager = ModelStorager(root_path=MLNODE_MODEL_STORAGE_DIR,
                              cache_size=MLNODE_MODEL_STORAGE_CACHESIZE)

# Usage:

# modelStorager.save_model(nn.Module, model_config, model_id, trial_id)
# model(TorchScript.ScriptModule), model_cfg(Dict) = modelStorager.load_model(model_config, model_id, trial_id)
# output = model(torch.randn(3, input_len, input_vars))
# modelStorager.delect_model(model_id, trial_id)
# modelStorager.delect_trial(trial_id)
