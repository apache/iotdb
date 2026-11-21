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

import torch
from huggingface_hub import PyTorchModelHubMixin
from safetensors.torch import load_file

from iotdb.ainode.core.model.timestral.config import TimestralConfig
from iotdb.ainode.core.model.timestral.model import TimestralModel


class TimestralForPrediction(TimestralModel, PyTorchModelHubMixin):
    """
    Timestral as an AINode model, loaded from a directory with the ``config.json`` and
    ``model.safetensors`` of a Hugging Face checkpoint such as ``mistralai/Timestral-209M``.

    The keys of ``config.json`` are the fields of :class:`TimestralConfig`.
    """

    def __init__(self, **fields):
        super().__init__(TimestralConfig.from_dict(fields))

    def _apply(self, fn, *args, **kwargs):
        module = super()._apply(fn, *args, **kwargs)
        # Recompute the rotary frequencies on the new device rather than moving them,
        # as Timestral's own loader does
        self.transformer.rope.rebuild_inv_freq()
        return module

    @classmethod
    def _load_as_safetensor(
        cls, model: "TimestralForPrediction", model_file: str, map_location: str, strict
    ) -> "TimestralForPrediction":
        state_dict = load_file(model_file, device=str(torch.device(map_location)))
        model.load_state_dict(state_dict, strict=True)
        model.transformer.rope.rebuild_inv_freq()
        return model.eval()
