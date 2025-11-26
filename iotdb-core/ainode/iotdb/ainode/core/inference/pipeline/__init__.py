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

from iotdb.ainode.core.model.sundial.pipeline_sundial import SundialPipeline
from iotdb.ainode.core.model.timer_xl.pipeline_timer import TimerPipeline


def get_pipeline(model_id, device):
    if model_id == "timer_xl":
        return TimerPipeline(model_id, device=device)
    elif model_id == "sundial":
        return SundialPipeline(model_id, device=device)
    else:
        raise ValueError(f"Unsupported model_id: {model_id} with pipeline")
