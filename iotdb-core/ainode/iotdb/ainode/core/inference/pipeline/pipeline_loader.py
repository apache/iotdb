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
from pathlib import Path

import torch

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_constants import ModelCategory
from iotdb.ainode.core.model.model_storage import ModelInfo
from iotdb.ainode.core.model.utils import import_class_from_path, temporary_sys_path

logger = Logger()


def load_pipeline(model_info: ModelInfo, device: torch.device, **model_kwargs):
    if model_info.model_type == "sktime":
        from iotdb.ainode.core.model.sktime.pipeline_sktime import SktimePipeline

        pipeline_cls = SktimePipeline
    elif model_info.category == ModelCategory.BUILTIN:
        module_name = (
            AINodeDescriptor().get_config().get_ain_models_builtin_dir()
            + "."
            + model_info.model_id
        )
        pipeline_cls = import_class_from_path(module_name, model_info.pipeline_cls)
    else:
        model_path = os.path.join(
            os.getcwd(),
            AINodeDescriptor().get_config().get_ain_models_dir(),
            model_info.category.value,
            model_info.model_id,
        )
        module_parent = str(Path(model_path).parent.absolute())
        with temporary_sys_path(module_parent):
            pipeline_cls = import_class_from_path(
                model_info.model_id, model_info.pipeline_cls
            )

    return pipeline_cls(model_info, device=device, **model_kwargs)
