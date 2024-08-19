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

import json
import os
import shutil
import threading
from typing import Dict, Tuple
from urllib.parse import urljoin, urlparse

import requests
import torch
import torch._dynamo
import torch.nn as nn
import yaml
from pylru import lrucache
from requests.adapters import HTTPAdapter

from iotdb.ainode.config import descriptor
from iotdb.ainode.constant import (OptionsKey, DEFAULT_MODEL_FILE_NAME,
                                   DEFAULT_CONFIG_FILE_NAME, DEFAULT_RECONNECT_TIMEOUT,
                                   DEFAULT_RECONNECT_TIMES, DEFAULT_CHUNK_SIZE)
from iotdb.ainode.exception import ModelNotExistError, InvaildUriError
from iotdb.ainode.log import logger
from iotdb.ainode.parser import parse_inference_config
from iotdb.ainode.util import pack_input_dict


class ModelStorage(object):
    _instance = None
    _first_init = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._first_init:
            self.__model_dir = os.path.join(os.getcwd(), descriptor.get_config().get_ain_models_dir())
            if not os.path.exists(self.__model_dir):
                try:
                    os.makedirs(self.__model_dir)
                except PermissionError as e:
                    logger.error(e)
                    raise e
            self.lock = threading.RLock()
            self.__model_cache = lrucache(descriptor.get_config().get_mn_model_storage_cache_size())
            self._first_init = True

    def _parse_uri(self, uri):
        '''
        Args:
            uri (str): uri to parse
        Returns:
            is_network_path (bool): True if the url is a network path, False otherwise
            parsed_uri (str): parsed uri to get related file
        '''
        # remove quotation mark in uri
        uri = uri[1:-1]

        parse_result = urlparse(uri)
        is_network_path = parse_result.scheme in ('http', 'https')
        if is_network_path:
            return True, uri

        # handle file:// in uri
        if parse_result.scheme == 'file':
            uri = uri[7:]

        # handle ~ in uri
        uri = os.path.expanduser(uri)
        return False, uri

    def _download_file(self, url: str, storage_path: str) -> None:
        '''
        Args:
            url: url of file to download
            storage_path: path to save the file
        Returns:
            None
        '''
        logger.debug(f"download file from {url} to {storage_path}")

        session = requests.Session()
        adapter = HTTPAdapter(max_retries=DEFAULT_RECONNECT_TIMES)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        response = session.get(url, timeout=DEFAULT_RECONNECT_TIMEOUT, stream=True)
        response.raise_for_status()

        with self.lock:
            with open(storage_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                    if chunk:
                        file.write(chunk)

        logger.debug(f"download file from {url} to {storage_path} success")

    def _register_model_from_network(self, uri: str, storage_path: str, model_storage_path: str,
                                     config_storage_path: str):
        '''
        Args:
            uri: network dir path of model to register, where model.pt and config.yaml are required,
                e.g. https://huggingface.co/user/modelname/resolve/main/
            storage_path: dir path to save the model
            model_storage_path: path to save model.pt
            config_storage_path: path to save config.yaml
        Returns:
            configs: TConfigs
            attributes: str
        '''
        # concat uri to get commplete url
        uri = uri if uri.endswith("/") else uri + "/"
        target_model_path = urljoin(uri, DEFAULT_MODEL_FILE_NAME)
        target_config_path = urljoin(uri, DEFAULT_CONFIG_FILE_NAME)

        # create storage dir if not exist
        with self.lock:
            if not os.path.exists(storage_path):
                os.makedirs(storage_path)

        # download config file
        self._download_file(target_config_path, config_storage_path)

        # read and parse config dict from config.yaml
        with open(config_storage_path, 'r', encoding='utf-8') as file:
            config_dict = yaml.safe_load(file)
        configs, attributes = parse_inference_config(config_dict)

        # if config.yaml is correct, download model file
        self._download_file(target_model_path, model_storage_path)
        return configs, attributes

    def _register_model_from_local(self, uri: str, storage_path: str, model_storage_path: str,
                                   config_storage_path: str):
        '''
        Args:
            uri: local dir path of model to register, where model.pt and config.yaml are required,
                e.g. /Users/admin/Desktop/dlinear
            storage_path: dir path to save the model
            model_storage_path: path to save model.pt
            config_storage_path: path to save config.yaml
        Returns:
            configs: TConfigs
            attributes: str
        '''
        # concat uri to get commplete path
        target_model_path = os.path.join(uri, DEFAULT_MODEL_FILE_NAME)
        target_config_path = os.path.join(uri, DEFAULT_CONFIG_FILE_NAME)

        # check if file exist
        exist_model_file = os.path.exists(target_model_path)
        exist_config_file = os.path.exists(target_config_path)

        if exist_model_file and exist_config_file:
            # copy config.yaml
            with self.lock:
                if not os.path.exists(storage_path):
                    os.makedirs(storage_path)

                logger.debug(f"copy file from {target_config_path} to {storage_path}")
                shutil.copy(target_config_path, config_storage_path)
                logger.debug(f"copy file from {target_config_path} to {storage_path} success")

            # read and parse config dict from config.yaml
            with open(config_storage_path, 'r', encoding='utf-8') as file:
                config_dict = yaml.safe_load(file)
            configs, attributes = parse_inference_config(config_dict)

            # if config.yaml is correct, copy model file
            with self.lock:
                logger.debug(f"copy file from {target_model_path} to {storage_path}")
                shutil.copy(target_model_path, model_storage_path)
                logger.debug(f"copy file from {target_model_path} to {storage_path} success")
        elif not exist_model_file or not exist_config_file:
            raise InvaildUriError(uri)

        return configs, attributes

    def register_model(self, model_id: str, uri: str):
        '''
        Args:
            model_id: id of model to register
            uri: network dir path or local dir path of model to register, where model.pt and config.yaml are required,
                e.g. https://huggingface.co/user/modelname/resolve/main/ or /Users/admin/Desktop/dlinear
        Returns:
            configs: TConfigs
            attributes: str
        '''
        storage_path = os.path.join(self.__model_dir, f'{model_id}')
        model_storage_path = os.path.join(storage_path, DEFAULT_MODEL_FILE_NAME)
        config_storage_path = os.path.join(storage_path, DEFAULT_CONFIG_FILE_NAME)

        is_network_path, uri = self._parse_uri(uri)

        if is_network_path:
            return self._register_model_from_network(uri, storage_path, model_storage_path, config_storage_path)
        else:
            return self._register_model_from_local(uri, storage_path, model_storage_path, config_storage_path)

    def save_model(self,
                   model: nn.Module,
                   model_config: Dict,
                   model_id: str,
                   trial_id: str) -> str:
        model_dir_path = os.path.join(self.__model_dir, f'{model_id}')
        logger.debug(f"save model {model_config} to {model_dir_path}")
        with self.lock:
            if not os.path.exists(model_dir_path):
                os.makedirs(model_dir_path)
        model_file_path = os.path.join(model_dir_path, f'{trial_id}.pt')

        # Note: model config for time series should contain 'input_len' and 'input_vars'
        sample_input = (
            pack_input_dict(
                torch.randn(1, model_config[OptionsKey.INPUT_LENGTH.name()], model_config[OptionsKey.INPUT_VARS.name()])
            )
        )
        with self.lock:
            torch.jit.save(torch.jit.trace(model, sample_input),
                           model_file_path,
                           _extra_files={'model_config': json.dumps(model_config)})
        return os.path.abspath(model_file_path)

    def load_model(
            self, file_path: str) -> Tuple[torch.jit.ScriptModule, Dict]:
        """
        Returns:
            jit_model: a ScriptModule contains model architecture and parameters, which can be deployed cross-platform
            model_config: a dict contains model attributes
        """
        logger.debug(f"load model from {file_path}")
        file_path = os.path.join(self.__model_dir, file_path)
        if file_path in self.__model_cache:
            return self.__model_cache[file_path]
        else:
            if not os.path.exists(file_path):
                raise ModelNotExistError(file_path)
            else:
                tmp_dict = {'model_config': ''}
                jit_model = torch.jit.load(file_path, _extra_files=tmp_dict)
                model_config = json.loads(tmp_dict['model_config'])
                self.__model_cache[file_path] = jit_model, model_config
                return jit_model, model_config

    def load_model_from_id(self, model_id: str, acceleration=False):
        """
        Returns:
            model: a ScriptModule contains model architecture and parameters, which can be deployed cross-platform
        """
        ain_models_dir = os.path.join(self.__model_dir, f'{model_id}')
        model_path = os.path.join(ain_models_dir, DEFAULT_MODEL_FILE_NAME)
        logger.debug(f"load model from {model_path}")
        if model_path in self.__model_cache:
            model = self.__model_cache[model_path]
            if isinstance(model, torch._dynamo.eval_frame.OptimizedModule) or not acceleration:
                return model
            else:
                model = torch.compile(model)
                self.__model_cache[model_path] = model
                return model
        else:
            if not os.path.exists(model_path):
                raise ModelNotExistError(model_path)
            else:
                model = torch.jit.load(model_path)
                if acceleration:
                    try:
                        model = torch.compile(model)
                    except:
                        logger.warning("acceleration failed, fallback to normal mode")
                self.__model_cache[model_path] = model
                return model

    def delete_model(self, model_id: str) -> None:
        '''
        Args:
            model_id: id of model to delete
        Returns:
            None
        '''
        storage_path = os.path.join(self.__model_dir, f'{model_id}')

        if os.path.exists(storage_path):
            for file_name in os.listdir(storage_path):
                self._remove_from_cache(os.path.join(storage_path, file_name))
            shutil.rmtree(storage_path)

    def delete_trial(self, model_id: str, trial_id: str) -> None:
        logger.debug(f"delete trial {trial_id} of model {model_id}")
        model_file_path = os.path.join(self.__model_dir, f'{model_id}', f'{trial_id}.pt')
        self._remove_from_cache(model_file_path)
        if os.path.exists(model_file_path):
            os.remove(model_file_path)

    def _remove_from_cache(self, file_path: str) -> None:
        if file_path in self.__model_cache:
            del self.__model_cache[file_path]


model_storage = ModelStorage()
