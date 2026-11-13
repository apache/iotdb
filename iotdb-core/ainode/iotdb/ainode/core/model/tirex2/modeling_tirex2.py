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

from iotdb.ainode.core.model.tirex2.model import TiRex2


class TiRex2ForPrediction(TiRex2, PyTorchModelHubMixin):
    """
    TiRex-2 as an AINode model, loaded from a directory with the ``config.json`` and
    ``model.safetensors`` of the Hugging Face checkpoint ``NX-AI/TiRex-2``.

    The keys of ``config.json`` are the constructor arguments of :class:`TiRex2`. The recurrent
    kernels follow the device the model is moved to: Triton kernels on CUDA when Triton is
    available, PyTorch kernels otherwise.
    """

    @classmethod
    def _load_as_safetensor(
        cls, model: "TiRex2ForPrediction", model_file: str, map_location: str, strict
    ) -> "TiRex2ForPrediction":
        # Load through load_state_dict, whose hooks convert the per-gate weights of the published
        # checkpoint to the fused layout of the recurrent layers; every weight must match.
        state_dict = load_file(model_file, device=str(torch.device(map_location)))
        model.load_state_dict(state_dict, strict=True)
        return model.eval()
