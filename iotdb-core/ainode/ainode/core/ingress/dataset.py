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
from torch.utils.data import Dataset

from ainode.core.ingress.iotdb import IoTDBTableModelDataset, IoTDBTreeModelDataset
from ainode.core.util.decorator import singleton


class BasicDatabaseDataset(Dataset):
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port


class BasicDatabaseForecastDataset(BasicDatabaseDataset):
    def __init__(self, ip: str, port: int, input_len: int, output_len: int):
        super().__init__(ip, port)
        self.input_len = input_len
        self.output_len = output_len


def register_dataset(key: str, dataset: Dataset):
    DatasetFactory().register(key, dataset)


@singleton
class DatasetFactory(object):

    def __init__(self):
        self.dataset_list = {
            "iotdb.table": IoTDBTableModelDataset,
            "iotdb.tree": IoTDBTreeModelDataset,
        }

    def register(self, key: str, dataset: Dataset):
        if key not in self.dataset_list:
            self.dataset_list[key] = dataset
        else:
            raise KeyError(f"Dataset {key} already exists")

    def deregister(self, key: str):
        del self.dataset_list[key]

    def get_dataset(self, key: str):
        if key not in self.dataset_list.keys():
            raise KeyError(f"Dataset {key} does not exist")
        return self.dataset_list[key]
