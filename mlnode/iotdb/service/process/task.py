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
import torch
import argparse
from iotdb.algorithm.forecast.models import *

from iotdb.algorithm.forecast.utils import parseModelConfig
from iotdb.data.utils import parseDataConfig, data_provider


def parseConfig(config):
    config = argparse.Namespace(**config)
    
    # default config
    config.use_gpu = False
    config.use_multi_gpu = False
    config.devices = [0]

    return config


class BasicTask(object):
    def __init__(self, config):
        self.config = parseConfig(config)
        self.model = self._build_model()
        self.device = self._acquire_device()
        self.model = self.model.to(self.device)
        self.dataset, self.dataloader = self._build_data()


    def _build_model(self):
        model_type = self.config.model_type
        model_config = parseModelConfig(self.config)
        model = eval(model_type)(model_config)
        return model


    def _build_data(self):
        data_config = parseDataConfig(self.config)
        dataset, dataloader = data_provider(data_config)
        return dataset, dataloader


    def _acquire_device(self):
        if self.config.use_gpu:
            os.environ["CUDA_VISIBLE_DEVICES"] = str(
                self.config.gpu) if not self.config.use_multi_gpu else self.config.devices
            device = torch.device('cuda:{}'.format(self.config.gpu))
            print('Use GPU: cuda:{}'.format(self.config.gpu))
        else:
            device = torch.device('cpu')
            print('Use CPU')
        return device


    def start(self):
        raise NotImplementedError


class ForecastingTrainingTask(BasicTask):
    def __init__(self, args):
        super(BasicTask, self).__init__(args)
    
    def start(self):
        pass


class ForecastingInferenceTask(BasicTask):
    def __init__(self, args):
        super(BasicTask, self).__init__(args)

    def start(self):
        pass