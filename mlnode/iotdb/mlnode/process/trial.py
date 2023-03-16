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
import pandas as pd
import torch
import torch.nn as nn

from torch.utils.data import Dataset, DataLoader

from iotdb.mlnode.log import logger
from iotdb.mlnode.client import DataNodeClient
from iotdb.mlnode.algorithm.utils.metric import *
from iotdb.mlnode.storage.model_storager import modelStorager
from iotdb.mlnode.datats.utils.timefeatures import data_transform, timestamp_transform


def parse_trial_config(**kwargs):
    support_cfg = {
        "batch_size": 32,
        "learning_rate": 0.0001,
        "epochs": 10,
        "input_len": 96,
        "pred_len": 96,
        "num_workers": 0,
        "use_gpu": False,
        "gpu": 0,
        "use_multi_gpu": False,
        "devices": [0],
        "metric_names": ["MSE"],  # TODO: CHECK
        "model_id": 'default',
        "trial_id": 'default_trial'
    }

    trial_config = {}

    for k, v in kwargs.items():
        if k in support_cfg.keys():
            if not isinstance(v, type(support_cfg[k])):
                raise RuntimeError(
                    'Trial config {} should have {} type, but got {} instead'
                    .format(k, type(support_cfg[k]).__name__, type(v).__name__)
                )
            trial_config[k] = v
    # TODO: check all param

    if trial_config['input_len'] <= 0:
        raise RuntimeError(
            'Trial config input_len should be positive integer but got {}'
            .format(trial_config['input_len'])
        )

    if trial_config['pred_len'] <= 0:
        raise RuntimeError(
            'Trial config pred_len should be positive integer but got {}'
            .format(trial_config['pred_len'])
        )

    for metric in trial_config['metric_names']:
        if metric not in all_metrics:
            raise RuntimeError(
                f'Unknown metric type: ({metric}), which'
                f' should be one of {all_metrics}'
            )
    return trial_config


class BasicTrial(object):
    def __init__(self, trial_configs: dict, model: nn.Module, model_configs: dict, dataset: Dataset, **kwargs):
        trial_configs = parse_trial_config(**trial_configs)  # TODO: remove all configs
        self.model_id = trial_configs['model_id']
        self.trial_id = trial_configs['trial_id']
        self.batch_size = trial_configs['batch_size']
        self.learning_rate = trial_configs['learning_rate']
        self.epochs = trial_configs['epochs']
        self.num_workers = trial_configs['num_workers']
        self.pred_len = trial_configs['pred_len']
        self.metric_names = trial_configs['metric_names']
        self.use_gpu = trial_configs['use_gpu']

        self.model = model
        self.model_configs = model_configs
        self.device = self._acquire_device()
        self.model = self.model.to(self.device)
        self.dataset = dataset

    def _build_data(self):
        raise NotImplementedError

    def _acquire_device(self):
        if self.use_gpu:
            raise NotImplementedError
        else:
            device = torch.device('cpu')
        return device

    def start(self):
        raise NotImplementedError


class ForecastingTrainingTrial(BasicTrial):
    def __init__(self, trial_configs, model, model_configs, dataset, **kwargs):
        super(ForecastingTrainingTrial, self) \
            .__init__(trial_configs, model, model_configs, dataset, **kwargs)
        self.dataloader = self._build_data()
        try:
            self.data_client = DataNodeClient(host='127.0.0.1', port=10730)
        except Exception:
            raise RuntimeError('Fail to establish connection with DataNode ("127.0.0.1", 10730)')

    def _build_data(self):
        train_loader = DataLoader(
            self.dataset,
            shuffle=True,
            drop_last=True,
            batch_size=self.batch_size,
            num_workers=self.num_workers
        )
        return train_loader

    def _train(self, model, optimizer, criterion, dataloader, epoch):
        model.train()
        train_loss = []
        epoch_time = time.time()

        for i, (batch_x, batch_y, batch_x_mark, batch_y_mark) in enumerate(dataloader):
            optimizer.zero_grad()
            batch_x = batch_x.float().to(self.device)
            batch_y = batch_y.float().to(self.device)

            batch_x_mark = batch_x_mark.float().to(self.device)
            batch_y_mark = batch_y_mark.float().to(self.device)

            # decoder input
            dec_inp = torch.zeros_like(batch_y[:, -self.pred_len:, :]).float()
            outputs = model(batch_x, batch_x_mark, dec_inp, batch_y_mark)
            outputs = outputs[:, -self.pred_len:]
            batch_y = batch_y[:, -self.pred_len:]
            loss = criterion(outputs, batch_y)
            train_loss.append(loss.item())

            if (i + 1) % 50 == 0:
                logger.info('\titers: {0}, epoch: {1} | loss: {2:.7f}'
                            .format(i + 1, epoch + 1, loss.item()))

            loss.backward()
            optimizer.step()

        train_loss = np.average(train_loss)
        # TODO: manage these training output
        logger.info('Epoch: {0} cost time: {1} | Train Loss: {2:.7f}'
                    .format(epoch + 1, time.time() - epoch_time, train_loss))
        return train_loss

    def _validate(self, model, criterion, dataloader, epoch):
        model.eval()
        val_loss = []
        metrics_dict = {name: [] for name in self.metric_names}
        for i, (batch_x, batch_y, batch_x_mark, batch_y_mark) in enumerate(dataloader):
            batch_x = batch_x.float().to(self.device)
            batch_y = batch_y.float().to(self.device)

            batch_x_mark = batch_x_mark.float().to(self.device)
            batch_y_mark = batch_y_mark.float().to(self.device)

            # decoder input
            dec_inp = torch.zeros_like(batch_y[:, -self.pred_len:, :]).float()
            outputs = model(batch_x, batch_x_mark, dec_inp, batch_y_mark)

            outputs = outputs[:, -self.pred_len:]
            batch_y = batch_y[:, -self.pred_len:]

            loss = criterion(outputs, batch_y)

            val_loss.append(loss.item())
            for name in self.metric_names:
                value = eval(name)(outputs.detach().cpu().numpy(),
                                   batch_y.detach().cpu().numpy())
                metrics_dict[name].append(value)

        for name, value_list in metrics_dict.items():
            metrics_dict[name] = np.average(value_list)

        # TODO: handle some exception
        self.data_client.record_model_metrics(
            modelId=self.model_id,
            trialId=self.trial_id,
            metrics=list(metrics_dict.keys()),
            values=list(metrics_dict.values())
        )
        val_loss = np.average(val_loss)
        logger.info('Epoch: {0} Vali Loss: {1:.7f}'.format(epoch + 1, val_loss))
        return val_loss

    def start(self):
        best_loss = np.inf
        criterion = torch.nn.MSELoss()
        optimizer = torch.optim.Adam(self.model.parameters(),
                                     lr=self.learning_rate)
        print("Start training...")
        for epoch in range(self.epochs):
            train_loss = self._train(self.model, optimizer, criterion, self.dataloader, epoch)
            # TODO: add validation methods
            val_loss = self._validate(self.model, criterion, self.dataloader, epoch)
            if val_loss < best_loss:
                best_loss = val_loss
                # TODO: generate trial id
                modelStorager.save_model(self.model, self.model_configs,
                                         model_id=self.model_id, trial_id=self.trial_id)
        logger.info(f'Trail: ({self.model_id}_{self.trial_id}) - Finished with best model saved successfully')
        return best_loss


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
        self.data, self.data_stamp = data_transform(
            data_raw)  # suppose data is in pandas.dataframe format, col 0 is timestamp
        self.model = modelStorager.load_best_model_by_id(self.model_id)

    def data_align(self, data, data_stamp):
        """
        data: L x C, ndarray
        time_stamp: L,
        """
        # data_stamp = pd.to_datetime(data_stamp.values, unit='ms', utc=True)
        #                .tz_convert('Asia/Shanghai')
        time_deltas = data_stamp.diff().dropna()
        mean_timedelta = time_deltas.mean()
        mean_timedelta = pd.Timedelta(milliseconds=mean_timedelta)
        if data.shape[0] < self.input_len:
            extra_len = self.input_len - data.shape[0]
            data = np.concatenate([data[:1, :].repeat(extra_len, 1), data], axis=0)
            extrapolated_timestamp = pd.date_range(data_stamp[0] - mean_timedelta,
                                                   periods=extra_len, freq=mean_timedelta)
            data_stamp = np.concatenate([extrapolated_timestamp, data_stamp])
        else:
            data = data[-self.input_len:, :]
            data_stamp = data_stamp[-self.input_len:]

        data = data[None, :]  # add batch dim
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

        data_stamp = pd.to_datetime(data_stamp.values, unit='ms', utc=True) \
            .tz_convert('Asia/Shanghai')
        out_timestamp_raw = pd.date_range(data_stamp[-1] + mean_timedelta,
                                          periods=self.output_len, freq=mean_timedelta)

        # convert into tensor
        data = torch.from_numpy(data).to(self.device)
        data_stamp = torch.from_numpy(timestamp_transform(data_stamp)).to(self.device)
        out_timestamp = torch.from_numpy(timestamp_transform(out_timestamp_raw)).to(self.device)

        output = self.inference(self.model, data, data_stamp, out_timestamp)
        logger.info("inference finished.")
        return output, out_timestamp_raw
