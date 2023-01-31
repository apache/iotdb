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
import time

import numpy as np
import multiprocessing as mp

from algorithm.forecast.models.DLinear import DLinear
from algorithm.forecast.utils import parseModelConfig
from data_provider.build_dataset_debug import debug_dataset


# from iotdb.data.utils import parseDataConfig, data_provider


def parseConfig(config):
    # config = argparse.Namespace(**config)
    
    # default config
    config.use_gpu = True
    config.use_multi_gpu = False
    config.devices = [0]
    config.gpu = 0

    config.model_type = 'DLinear'

    return config


class BasicTask(object):
    def __init__(self, configs):
        self.configs = parseConfig(configs)
        self.model = self._build_model()
        self.device = self._acquire_device()
        self.model = self.model.to(self.device)
        self.dataset, self.dataloader = self._build_data()


    def _build_model(self):
        model_type = self.configs.model_type
        model_config = parseModelConfig(self.configs)
        model = eval(model_type)(model_config)

        return model


    def _build_data(self):
        # data_config = parseDataConfig(self.config)
        # dataset, dataloader = data_provider(data_config)
        dataset, dataloader = debug_dataset()
        return dataset, dataloader


    def _acquire_device(self):
        if self.configs.use_gpu:
            os.environ["CUDA_VISIBLE_DEVICES"] = str(
                self.configs.gpu) if not self.configs.use_multi_gpu else self.configs.devices
            device = torch.device('cuda:{}'.format(self.configs.gpu))
            print('Use GPU: cuda:{}'.format(self.configs.gpu))
        else:
            device = torch.device('cpu')
            print('Use CPU')
        return device


    def start(self):
        raise NotImplementedError


class ForecastingTrainingTask(BasicTask):
    def __init__(self, args, task_map, task_id):
        super(ForecastingTrainingTask, self).__init__(args)
        self.task_map = task_map
        self.pid = os.getpid()
        self.task_id = task_id
        
    def train(self, model, optimizer, criterion, dataloader, configs, epoch):
        model.train()
        train_loss = []

        epoch_time = time.time()
        for i, (batch_x, batch_y, batch_x_mark, batch_y_mark) in enumerate(dataloader):
            optimizer.zero_grad()

            batch_x = batch_x.float().to(configs.device)
            batch_y = batch_y.float().to(configs.device)

            batch_x_mark = batch_x_mark.float().to(configs.device)
            batch_y_mark = batch_y_mark.float().to(configs.device)

            # decoder input
            dec_inp = torch.zeros_like(batch_y[:, -configs.pred_len:, :]).float()
            dec_inp = torch.cat([batch_y[:, :configs.label_len, :], dec_inp], dim=1).float().to(configs.device)
            outputs = model(batch_x, batch_x_mark, dec_inp, batch_y_mark)

            outputs = outputs[:, -configs.pred_len:]
            batch_y = batch_y[:, -configs.pred_len:]
            loss = criterion(outputs, batch_y)
            train_loss.append(loss.item())

            #if (i + 1) % 50 == 0:
            #    print('\titers: {0}, epoch: {1} | loss: {2:.7f}'.format(i + 1, epoch + 1, loss.item()))
            
            loss.backward()
            optimizer.step()
        
        train_loss = np.average(train_loss)
        # print('Epoch: {0} cost time: {1} | Train Loss: {2:.7f}'.format(epoch + 1, time.time() - epoch_time, train_loss))

        return train_loss
    
    def start(self):
        optimizer = torch.optim.Adam(self.model.parameters(), lr=self.configs.learning_rate)
        criterion = torch.nn.MSELoss()

        for epoch in range(self.configs.epochs):
            train_loss = self.train(self.model, optimizer, criterion, self.dataloader, self.configs, epoch) 
        self.task_map[self.task_id][self.pid] = 'finished'
        return 
        

class ForecastingInferenceTask(BasicTask):
    def __init__(self, args):
        """
        model configs
        model path
        model id
        data 
        """
        super(BasicTask, self).__init__(args)

    def start(self):
        pass

if __name__ == '__main__':
    from data_provider.build_dataset_debug import *
    configs = debug_configs()
    task = ForecastingTrainingTask(configs)
    task.start()