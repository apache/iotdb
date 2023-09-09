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
import time
from typing import Dict

import torch.nn as nn
from iotdb.mlnode.constant import ModelInputName

from iotdb.mlnode.config import descriptor
from iotdb.mlnode.exception import ModelNotExistError
from iotdb.mlnode.storage import model_storage


class ExampleModel(nn.Module):
    def __init__(self):
        super(ExampleModel, self).__init__()
        self.layer = nn.Identity()

    def forward(self, input_dict: Dict):
        # x: [Batch, Input length, Channel]
        x = input_dict[ModelInputName.DATA_X.value]
        return self.layer(x)


model = ExampleModel()
model_config = {
    'input_length': 1,
    'input_vars': 1,
    'id': time.time()
}


def test_save_model():
    trial_id = 'tid_0'
    model_id = 'mid_test_model_save'
    model_storage.save_model(model, model_config, model_id=model_id, trial_id=trial_id)
    assert os.path.exists(
        os.path.join(descriptor.get_config().get_mn_model_storage_dir(), f'{model_id}', f'{trial_id}.pt'))


def test_load_model():
    trial_id = 'tid_0'
    model_id = 'mid_test_model_load'

    model_file_path = os.path.join(f'{model_id}', f'{trial_id}.pt')

    model_storage.save_model(model, model_config, model_id=model_id, trial_id=trial_id)
    _, model_config_loaded = model_storage.load_model(model_file_path)
    assert model_config == model_config_loaded


def test_load_not_exist_model():
    trial_id = 'dummy_trial'
    model_id = 'dummy_model'
    try:
        model_storage.load_model(os.path.join(f'{model_id}', f'{trial_id}.pt'))
    except Exception as e:
        assert e.message == ModelNotExistError(
            os.path.join(os.getcwd(), descriptor.get_config().get_mn_model_storage_dir(),
                         model_id, f'{trial_id}.pt')).message


def test_delete_model():
    trial_id1 = 'tid_1'
    trial_id2 = 'tid_2'
    model_id = 'mid_test_model_delete'
    model_storage.save_model(model, model_config, model_id=model_id, trial_id=trial_id1)
    model_storage.save_model(model, model_config, model_id=model_id, trial_id=trial_id2)
    model_storage.delete_model(model_id=model_id)
    assert not os.path.exists(
        os.path.join(descriptor.get_config().get_mn_model_storage_dir(), f'{model_id}', f'{trial_id1}.pt'))
    assert not os.path.exists(
        os.path.join(descriptor.get_config().get_mn_model_storage_dir(), f'{model_id}', f'{trial_id2}.pt'))
    assert not os.path.exists(os.path.join(descriptor.get_config().get_mn_model_storage_dir(), f'{model_id}'))


def test_delete_trial():
    trial_id = 'tid_0'
    model_id = 'mid_test_model_delete'
    model_storage.save_model(model, model_config, model_id=model_id, trial_id=trial_id)
    model_storage.delete_trial(model_id=model_id, trial_id=trial_id)
    assert not os.path.exists(
        os.path.join(descriptor.get_config().get_mn_model_storage_dir(), f'{model_id}', f'{trial_id}.pt'))
