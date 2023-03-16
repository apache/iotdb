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
import os.path

import torchvision
from iotdb.mlnode.model_storage import model_storage


def test_save_model():
    model = torchvision.models.resnet50()
    model_config = {
        'model_name': 'resnet50',
        'task': 'cv',
        'input_len': 1
    }
    res = model_storage.save_model(model, model_config, model_id='resnet50', trial_id='0')
    print(res)
    # assert os.path.exists(os.path.join(model_storage.__model_dir, 'model_id'))

