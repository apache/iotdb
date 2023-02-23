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
import pandas as pd
import multiprocessing as mp

from algorithm.forecast.models.DLinear import DLinear
from algorithm.forecast.utils import parseModelConfig
from data_provider.build_dataset_debug import debug_dataset
from data_provider.offline_dataset import data_transform, timestamp_transform
from ModelManager import modelManager

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


class BasicTrial(object):
    def __init__(self, configs):
        self.configs = parseConfig(configs)
        self.model = self._build_model()
        self.device = self._acquire_device()
        self.model = self.model.to(self.device)
        self.dataset, self.dataloader, self.val_dataset, self.val_loader = self._build_data()

        self.model_id = 1


    def _build_model(self):
        model_type = self.configs.model_type
        model_config = parseModelConfig(self.configs)
        model = eval(model_type)(model_config)

        return model


    def _build_data(self):
        # data_config = parseDataConfig(self.config)
        # dataset, dataloader = data_provider(data_config)
        dataset, dataloader, testset, testloader = debug_dataset()
        return dataset, dataloader, testset, testloader


    def _acquire_device(self):
        if self.configs.use_gpu:
            os.environ["CUDA_VISIBLE_DEVICES"] = str(
                self.configs.gpu) if not self.configs.use_multi_gpu else self.configs.devices
            device = torch.device('cuda:{}'.format(self.configs.gpu))
            # print('Use GPU: cuda:{}'.format(self.configs.gpu))
        else:
            device = torch.device('cpu')
            # print('Use CPU')
        return device


    def start(self):
        raise NotImplementedError


class ForecastingTrainingTrial(BasicTrial):
    def __init__(self, args):
        super(ForecastingTrainingTrial, self).__init__(args)
        
    def train(self, model, optimizer, criterion, dataloader, configs, epoch):
        model.train()
        train_loss = []
        print("start training...")

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

            if (i + 1) % 50 == 0:
               print('\titers: {0}, epoch: {1} | loss: {2:.7f}'.format(i + 1, epoch + 1, loss.item()))
            
            loss.backward()
            optimizer.step()

        
        train_loss = np.average(train_loss)
        print('Epoch: {0} cost time: {1} | Train Loss: {2:.7f}'.format(epoch + 1, time.time() - epoch_time, train_loss))

        return train_loss
    
    def validate(self, model, criterion, dataloader, configs, epoch):
        model.eval()
        val_loss = []
        for i, (batch_x, batch_y, batch_x_mark, batch_y_mark) in enumerate(dataloader):

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
            val_loss.append(loss.item())         

        val_loss = np.average(val_loss)
        print('Epoch: {0} Vali Loss: {1:.7f}'.format(epoch + 1, val_loss))
        return val_loss
    

    def start(self):
        optimizer = torch.optim.Adam(self.model.parameters(), lr=self.configs.learning_rate)
        criterion = torch.nn.MSELoss()

        best_loss = np.inf
        for epoch in range(self.configs.epochs):
            train_loss = self.train(self.model, optimizer, criterion, self.dataloader, self.configs, epoch) 
            val_loss = self.validate(self.model, criterion, self.val_loader, self.configs, epoch)
            if val_loss < best_loss:
                best_loss = val_loss
                modelManager.saveModel(self.model, self.model_id, 1)
        return val_loss
        

class ForecastingInferenceTrial(BasicTrial):
    def __init__(self, args, data_raw):
        """
        model configs
        model path
        model id
        data 
        """
        super(ForecastingInferenceTrial, self).__init__(args)
        self.input_len = args.seq_len
        self.output_len = args.pred_len
        self.data, self.data_stamp = data_transform(data_raw) # suppose data is in pandas.dataframe format, col 0 is timestamp
        self.model = torch.load('ModelData/1/1/checkpoint.pth')


    def data_align(self, data, data_stamp):
        """
        data: L x C, ndarray
        time_stamp: L, 
        """
        # data_stamp = pd.to_datetime(data_stamp.values, unit='ms', utc=True).tz_convert('Asia/Shanghai')
        time_deltas = data_stamp.diff().dropna()
        mean_timedelta = time_deltas.mean()
        mean_timedelta = pd.Timedelta(milliseconds=mean_timedelta)
        if data.shape[0] < self.input_len:
            extra_len = self.input_len - data.shape[0]
            data = np.concatenate([data[:1, :].repeat(extra_len, 1), data], axis=0)
            extrapolated_timestamp = pd.date_range(data_stamp[0] - mean_timedelta, periods=extra_len, freq=mean_timedelta)
            data_stamp = np.concatenate([extrapolated_timestamp, data_stamp])
        else:
            data = data[-self.input_len:, :]
            data_stamp = data_stamp[-self.input_len:]

        data = data[None, :] # add batch dim
        return data, data_stamp

    def inference(self, model, data, data_stamp, out_timestamp):
        model.eval()
        B, L, C = data.shape
        dec_inp = torch.zeros((B, self.output_len, C)).to(self.device)
        output = model(data, data_stamp, dec_inp, out_timestamp)
        
        return output
        

    def start(self):
        data, data_stamp = self.data_align(self.data, self.data_stamp)
        # compute output time stamp
        time_deltas = data_stamp.diff().dropna()
        mean_timedelta = time_deltas.mean()
        mean_timedelta = pd.Timedelta(milliseconds=mean_timedelta)

        data_stamp = pd.to_datetime(data_stamp.values, unit='ms', utc=True).tz_convert('Asia/Shanghai')
        out_timestamp_raw = pd.date_range(data_stamp[-1] + mean_timedelta, periods=self.output_len, freq=mean_timedelta)

        # convert into tensor
        data = torch.from_numpy(data).to(self.device)
        data_stamp = torch.from_numpy(timestamp_transform(data_stamp)).to(self.device)
        out_timestamp = torch.from_numpy(timestamp_transform(out_timestamp_raw)).to(self.device)

        output = self.inference(self.model, data, data_stamp, out_timestamp)
        print("inference finished.")
        return output, out_timestamp_raw
        

if __name__ == '__main__':
    from data_provider.build_dataset_debug import *
    configs = default_configs()
    data = debug_inference_data()
    trial = ForecastingInferenceTrial(configs, data)
    trial.start()