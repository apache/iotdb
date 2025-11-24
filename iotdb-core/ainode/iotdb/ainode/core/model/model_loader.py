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
from typing import Any

import torch
from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    AutoModelForNextSentencePrediction,
    AutoModelForSeq2SeqLM,
    AutoModelForSequenceClassification,
    AutoModelForTimeSeriesPrediction,
    AutoModelForTokenClassification,
)

from iotdb.ainode.core.exception import ModelNotExistError
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_enums import ModelCategory
from iotdb.ainode.core.model.model_info import ModelInfo
from iotdb.ainode.core.model.model_storage import ModelStorage
from iotdb.ainode.core.model.sktime.modeling_sktime import create_sktime_model

logger = Logger()


class ModelLoader:
    """Model loader - unified interface for loading different types of models"""

    def __init__(self, storage: ModelStorage):
        self.storage = storage

    def load_model(self, model_id: str, **kwargs) -> Any:
        # Lazy registration: if it's a Transformers model and not registered, register it first
        model_info = self.storage.ensure_transformers_registered(model_id)
        if not model_info:
            logger.error(
                f"Model {model_id} failed to register to Transformers, cannot load."
            )
            return None

        if model_info.auto_map is not None:
            model = self.load_model_from_transformers(model_info, **kwargs)
        else:
            if model_info.model_type == "sktime":
                model = create_sktime_model(model_id)
            else:
                model = self.load_model_from_pt(model_info, **kwargs)

        logger.info(f"Model {model_id} loaded to device {model.device} successfully.")
        return model

    def load_model_from_transformers(self, model_info: ModelInfo, **kwargs):
        model_config, load_class = None, None
        device_map = kwargs.get("device_map", "cpu")
        trust_remote_code = kwargs.get("trust_remote_code", True)
        train_from_scratch = kwargs.get("train_from_scratch", False)

        if model_info.category == ModelCategory.BUILTIN:
            if model_info.model_id == "timerxl":
                from iotdb.ainode.core.model.timerxl.configuration_timer import (
                    TimerConfig,
                )

                model_config = TimerConfig()
                from iotdb.ainode.core.model.timerxl.modeling_timer import (
                    TimerForPrediction,
                )

                load_class = TimerForPrediction
            elif model_info.model_id == "sundial":
                from iotdb.ainode.core.model.sundial.configuration_sundial import (
                    SundialConfig,
                )

                model_config = SundialConfig()
                from iotdb.ainode.core.model.sundial.modeling_sundial import (
                    SundialForPrediction,
                )

                load_class = SundialForPrediction
            else:
                logger.error(
                    f"Unsupported built-in Transformers model {model_info.model_id}."
                )
        else:
            model_config = AutoConfig.from_pretrained(model_info.path)
            if (
                type(model_config)
                in AutoModelForTimeSeriesPrediction._model_mapping.keys()
            ):
                load_class = AutoModelForTimeSeriesPrediction
            elif (
                type(model_config)
                in AutoModelForNextSentencePrediction._model_mapping.keys()
            ):
                load_class = AutoModelForNextSentencePrediction
            elif type(model_config) in AutoModelForSeq2SeqLM._model_mapping.keys():
                load_class = AutoModelForSeq2SeqLM
            elif (
                type(model_config)
                in AutoModelForSequenceClassification._model_mapping.keys()
            ):
                load_class = AutoModelForSequenceClassification
            elif (
                type(model_config)
                in AutoModelForTokenClassification._model_mapping.keys()
            ):
                load_class = AutoModelForTokenClassification
            else:
                load_class = AutoModelForCausalLM

        if train_from_scratch:
            model = load_class.from_config(
                model_config, trust_remote_code=trust_remote_code, device_map=device_map
            )
        else:
            model = load_class.from_pretrained(
                model_info.path,
                trust_remote_code=trust_remote_code,
                device_map=device_map,
            )

        return model

    def load_model_from_pt(self, model_info: ModelInfo, **kwargs):
        device_map = kwargs.get("device_map", "cpu")
        acceleration = kwargs.get("acceleration", False)
        model_path = os.path.join(model_info.path, "model.pt")
        if not os.path.exists(model_path):
            logger.error(f"Model file not found at {model_path}.")
            raise ModelNotExistError(model_path)
        model = torch.jit.load(model_path)
        if (
            isinstance(model, torch._dynamo.eval_frame.OptimizedModule)
            or not acceleration
        ):
            return model
        try:
            model = torch.compile(model)
        except Exception as e:
            logger.warning(f"acceleration failed, fallback to normal mode: {str(e)}")
        return model.to(device_map)

    def load_model_for_efficient_inference(self):
        # TODO: An efficient model loading method for inference based on model_arguments
        pass

    def load_model_for_powerful_finetune(self):
        # TODO: An powerful model loading method for finetune based on model_arguments
        pass

    def unload_model(self):
        pass
