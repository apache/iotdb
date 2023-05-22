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
import time
from abc import abstractmethod
from typing import Dict, Tuple

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

from iotdb.mlnode.algorithm.metric import all_metrics, build_metrics
from iotdb.mlnode.client import client_manager
from iotdb.mlnode.log import logger
from iotdb.mlnode.storage import model_storage
from iotdb.thrift.common.ttypes import TrainingState


def _parse_trial_config(**kwargs):
    support_cfg = {
        "batch_size": 32,
        "learning_rate": 0.0001,
        "epochs": 10,
        "input_len": 96,
        "pred_len": 96,
        "num_workers": 0,
        "use_gpu": False,
        # "gpu": 0,
        # "use_multi_gpu": False,
        # "devices": [0],
        "metric_names": ["MSE"],
        "model_id": 'default',
        "trial_id": 'default_trial'
    }

    trial_config = {}

    for k, v in kwargs.items():
        if k in support_cfg.keys():
            if not isinstance(v, type(support_cfg[k])):
                raise RuntimeError(
                    'Trial config {} should have {} type, but got {} instead'.format(k, type(support_cfg[k]).__name__,
                                                                                     type(v).__name__)
                )
            trial_config[k] = v

    if trial_config['input_len'] <= 0:
        raise RuntimeError(
            'Trial config input_len should be positive integer but got {}'.format(trial_config['input_len'])
        )

    if trial_config['pred_len'] <= 0:
        raise RuntimeError(
            'Trial config pred_len should be positive integer but got {}'.format(trial_config['pred_len'])
        )

    for metric in trial_config['metric_names']:
        if metric not in all_metrics:
            raise RuntimeError(
                f'Unknown metric type: ({metric}), which'
                f' should be one of {all_metrics}'
            )
    return trial_config


class BasicTrial(object):
    def __init__(
            self,
            task_configs: Dict,
            model: nn.Module,
            model_configs: Dict,
            dataset: Dataset
    ):
        self.trial_configs = task_configs
        self.model_id = task_configs['model_id']
        self.trial_id = task_configs['trial_id']
        self.batch_size = task_configs['batch_size']
        self.learning_rate = task_configs['learning_rate']
        self.epochs = task_configs['epochs']
        self.num_workers = task_configs['num_workers']
        self.pred_len = task_configs['pred_len']
        self.metric_names = task_configs['metric_names']
        self.use_gpu = task_configs['use_gpu']
        self.model = model
        self.model_configs = model_configs

        self.device = self.__acquire_device()
        self.model = self.model.to(self.device)
        self.dataset = dataset

    def __acquire_device(self):
        if self.use_gpu:
            raise NotImplementedError
        else:
            device = torch.device('cpu')
        return device

    def _build_dataloader(self) -> DataLoader:
        """
        Returns:
            training dataloader built with the dataset
        """
        return DataLoader(
            self.dataset,
            shuffle=True,
            drop_last=True,
            batch_size=self.batch_size,
            num_workers=self.num_workers
        )

    @abstractmethod
    def start(self):
        raise NotImplementedError


class ForecastingTrainingTrial(BasicTrial):
    def __init__(
            self,
            task_configs: Dict,
            model: nn.Module,
            model_configs: Dict,
            dataset: Dataset,
    ):
        """
        A training trial, accept all parameters needed and train a single model.

        Args:
            trial_configs: dict of trial's configurations
            model: torch.nn.Module
            model_configs: dict of model's configurations
            dataset: training dataset
            **kwargs:
        """
        super(ForecastingTrainingTrial, self).__init__(task_configs, model, model_configs, dataset)

        self.dataloader = self._build_dataloader()
        self.datanode_client = client_manager.borrow_data_node_client()
        self.confignode_client = client_manager.borrow_config_node_client()
        self.criterion = torch.nn.MSELoss()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=self.learning_rate)
        self.metrics_dict = build_metrics(self.metric_names)

    def train(self, epoch: int) -> float:
        self.model.train()
        train_loss = []
        epoch_time = time.time()

        for i, (batch_x, batch_y, batch_x_mark, batch_y_mark) in enumerate(self.dataloader):
            self.optimizer.zero_grad()
            batch_x = batch_x.float().to(self.device)
            batch_y = batch_y.float().to(self.device)

            batch_x_mark = batch_x_mark.float().to(self.device)
            batch_y_mark = batch_y_mark.float().to(self.device)

            # decoder input
            dec_inp = torch.zeros_like(batch_y[:, -self.pred_len:, :]).float()
            outputs = self.model(batch_x, batch_x_mark, dec_inp, batch_y_mark)

            outputs = outputs[:, -self.pred_len:]
            batch_y = batch_y[:, -self.pred_len:]

            loss = self.criterion(outputs, batch_y)
            train_loss.append(loss.item())

            if (i + 1) % 500 == 0:
                logger.info('\titers: {0}, epoch: {1} | loss: {2:.7f}'
                            .format(i + 1, epoch + 1, loss.item()))

            loss.backward()
            self.optimizer.step()

        train_loss = np.average(train_loss)
        logger.info('Epoch: {0} cost time: {1} | Train Loss: {2:.7f}'
                    .format(epoch + 1, time.time() - epoch_time, train_loss))
        return train_loss

    def vali(self, epoch: int) -> Tuple[float, Dict]:
        self.model.eval()
        val_loss = []
        metrics_value_dict = {name: [] for name in self.metric_names}
        for i, (batch_x, batch_y, batch_x_mark, batch_y_mark) in enumerate(self.dataloader):
            batch_x = batch_x.float().to(self.device)
            batch_y = batch_y.float().to(self.device)

            batch_x_mark = batch_x_mark.float().to(self.device)
            batch_y_mark = batch_y_mark.float().to(self.device)

            # decoder input
            dec_inp = torch.zeros_like(batch_y[:, -self.pred_len:, :]).float()
            outputs = self.model(batch_x, batch_x_mark, dec_inp, batch_y_mark)

            outputs = outputs[:, -self.pred_len:]
            batch_y = batch_y[:, -self.pred_len:]

            loss = self.criterion(outputs, batch_y)
            val_loss.append(loss.item())

            for name in self.metric_names:
                metric = self.metrics_dict[name]
                value = metric(outputs.detach().cpu().numpy(), batch_y.detach().cpu().numpy())
                metrics_value_dict[name].append(value)

        for name, value_list in metrics_value_dict.items():
            metrics_value_dict[name] = np.average(value_list)

        self.datanode_client.record_model_metrics(
            model_id=self.model_id,
            trial_id=self.trial_id,
            metrics=list(metrics_value_dict.keys()),
            values=list(metrics_value_dict.values())
        )
        val_loss = np.average(val_loss)
        logger.info('Epoch: {0} Vali Loss: {1:.7f}'.format(epoch + 1, val_loss))
        return val_loss, metrics_value_dict

    def start(self) -> float:
        """
        Start training with the specified parameters, save the best model and report metrics to the db.
        """
        try:
            self.confignode_client.update_model_state(self.model_id, TrainingState.RUNNING)
            best_loss = np.inf
            best_metrics_dict = None
            model_path = None
            for epoch in range(self.epochs):
                self.train(epoch)
                val_loss, metrics_dict = self.vali(epoch)
                if val_loss < best_loss:
                    best_loss = val_loss
                    best_metrics_dict = metrics_dict
                    model_path = model_storage.save_model(self.model,
                                                          self.model_configs,
                                                          model_id=self.model_id,
                                                          trial_id=self.trial_id)

            logger.info(f'Trial: ({self.model_id}_{self.trial_id}) - Finished with best model saved successfully')
            model_info = {}
            model_info.update(best_metrics_dict)
            model_info.update(self.trial_configs)
            model_info['model_path'] = model_path
            self.confignode_client.update_model_info(self.model_id, self.trial_id, model_info)
            self.confignode_client.update_model_state(self.model_id, TrainingState.FINISHED, self.trial_id)
            return best_loss
        except Exception as e:
            logger.warn(e)
            self.confignode_client.update_model_state(self.model_id, TrainingState.FAILED)
            raise e
