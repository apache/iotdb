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
from abc import abstractmethod
from typing import Dict, List

import numpy as np

all_metrics = ['RSE', 'CORR', 'MAE', 'MSE', 'RMSE', 'MAPE', 'MSPE']


class Metric(object):
    def __call__(self, pred, ground_truth):
        return self.calculate(pred, ground_truth)

    @abstractmethod
    def calculate(self, pred, ground_truth):
        pass


class RSE(Metric):
    def calculate(self, pred, ground_truth):
        return np.sqrt(np.sum((ground_truth - pred) ** 2)) / np.sqrt(np.sum((ground_truth - ground_truth.mean()) ** 2))


class CORR(Metric):
    def calculate(self, pred, ground_truth):
        u = ((ground_truth - ground_truth.mean(0)) * (pred - pred.mean(0))).sum(0)
        d = np.sqrt(((ground_truth - ground_truth.mean(0)) ** 2 * (pred - pred.mean(0)) ** 2).sum(0))
        return (u / d).mean(-1)


class MAE(Metric):
    def calculate(self, pred, ground_truth):
        return np.mean(np.abs(pred - ground_truth))


class MSE(Metric):
    def calculate(self, pred, ground_truth):
        return np.mean((pred - ground_truth) ** 2)


class RMSE(Metric):
    def calculate(self, pred, ground_truth):
        mse = MSE()
        return np.sqrt(mse(pred, ground_truth))


class MAPE(Metric):
    def calculate(self, pred, ground_truth):
        return np.mean(np.abs((pred - ground_truth) / ground_truth))


class MSPE(Metric):
    def calculate(self, pred, ground_truth):
        return np.mean(np.square((pred - ground_truth) / ground_truth))


def build_metrics(metric_names: List) -> Dict[str, Metric]:
    metrics_dict = {}
    for metric_name in metric_names:
        metrics_dict[metric_name] = eval(metric_name)()
    return metrics_dict
