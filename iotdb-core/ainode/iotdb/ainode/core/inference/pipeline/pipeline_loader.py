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

from pathlib import Path

from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_constants import ModelCategory
from iotdb.ainode.core.model.model_storage import ModelInfo
from iotdb.ainode.core.model.utils import temporary_sys_path, import_class_from_path

logger = Logger()


def load_pipeline(model_info: ModelInfo, device: str, **kwargs):
    if model_info.category == ModelCategory.BUILTIN:
        if model_info.model_id == "timer_xl":
            from iotdb.ainode.core.model.timer_xl.pipeline_timer import TimerPipeline
            pipeline_cls = TimerPipeline
        elif model_info.model_id == "sundial":
            from iotdb.ainode.core.model.sundial.pipeline_sundial import SundialPipeline
            pipeline_cls = SundialPipeline
        else:
            logger.error(
                f"Unsupported built-in model {model_info.model_id}."
            )
            return None
    else:
        module_parent = str(Path(model_info.path).parent.absolute())
        with temporary_sys_path(module_parent):
            pipeline_cls = import_class_from_path(
                model_info.model_id, model_info.pipeline_cls
            )

    return pipeline_cls(model_info.model_id, device=device)
