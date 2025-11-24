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

import pandas as pd
import torch

from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline
from iotdb.ainode.core.util.serde import convert_to_binary


class TimerxlPipeline(ForecastPipeline):
    def __init__(self, model_id, **infer_kwargs):
        super().__init__(model_id, infer_kwargs=infer_kwargs)

    def _preprocess(self, inputs):
        return super()._preprocess(inputs)

    def infer(self, inputs, **infer_kwargs):
        predict_length = infer_kwargs.get("predict_length", 96)
        revin = infer_kwargs.get("revin", True)

        input_ids = self._preprocess(inputs)
        output = self.model.generate(
            input_ids, max_new_tokens=predict_length, revin=revin
        )
        return self._postprocess(output)

    def _postprocess(self, output: torch.Tensor):
        return output
