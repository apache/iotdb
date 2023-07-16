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

from iotdb.mlnode.algorithm.factory import create_forecast_model
from iotdb.mlnode.algorithm.hyperparameter import HyperparameterName
from iotdb.mlnode.algorithm.metric import build_metrics, forecast_metric_names
from iotdb.mlnode.client import client_manager
from iotdb.mlnode.constant import OptionsKey
from iotdb.mlnode.das.dataset import TsDataset
from iotdb.mlnode.log import logger
from iotdb.mlnode.parser import ForecastTaskOptions
from iotdb.mlnode.storage import model_storage
from iotdb.thrift.common.ttypes import TrainingState


class BasicTrial(object):
    def __init__(
            self,
            model_id: str,
            model: nn.Module,
            task_configs: Dict,
            dataset: Dataset
    ):
        self.model_id = model_id
        self.dataset = dataset
        self.trial_configs = task_configs
        self.batch_size = task_configs[HyperparameterName.BATCH_SIZE.name()]
        self.learning_rate = task_configs[HyperparameterName.LEARNING_RATE.name()]
        self.epochs = task_configs[HyperparameterName.EPOCHS.name()]
        self.num_workers = task_configs[HyperparameterName.NUM_WORKERS.name()]
        self.use_gpu = task_configs[HyperparameterName.USE_GPU.name()]
        self.device = self.__acquire_device()
        self.model = model.to(self.device)

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
            trial_id: str,
            model_id: str,
            task_options: ForecastTaskOptions,
            model_hyperparameters: Dict,
            task_hyperparameters: Dict,
            dataset: TsDataset
    ):
        """
        A training trial, accept all parameters needed and train a single model.
        """
        self.model_configs = model_hyperparameters
        self.model_configs[HyperparameterName.INPUT_VARS.value] = dataset.get_variable_num()
        self.model_configs[OptionsKey.INPUT_LENGTH.value] = task_options.input_length
        self.model_configs[OptionsKey.PREDICT_LENGTH.value] = task_options.predict_length
        model = create_forecast_model(task_options, self.model_configs)

        super(ForecastingTrainingTrial, self).__init__(model_id, model, task_hyperparameters, dataset)
        self.trial_id = trial_id
        self.pred_len = task_options.predict_length
        self.dataloader = self._build_dataloader()
        self.datanode_client = client_manager.borrow_data_node_client()
        self.configNode_client = client_manager.borrow_config_node_client()
        self.criterion = torch.nn.MSELoss()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=self.learning_rate)
        self.metrics_dict = build_metrics(forecast_metric_names)

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
        metrics_value_dict = {name: [] for name in forecast_metric_names}
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

            for name in forecast_metric_names:
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
            self.configNode_client.update_model_state(self.model_id, TrainingState.RUNNING)
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
            self.configNode_client.update_model_info(self.model_id, self.trial_id, model_info)
            self.configNode_client.update_model_state(self.model_id, TrainingState.FINISHED, self.trial_id)
            return best_loss
        except Exception as e:
            logger.warn(e)
            self.configNode_client.update_model_state(self.model_id, TrainingState.FAILED)
            raise e
