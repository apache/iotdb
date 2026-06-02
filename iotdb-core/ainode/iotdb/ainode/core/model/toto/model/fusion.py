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
# This file includes code derived from DataDog/toto
# (https://github.com/DataDog/toto), licensed under the Apache-2.0 License.
# Copyright 2025 Datadog, Inc.

from typing import Optional

import torch
import torch.nn.functional as F
from jaxtyping import Float


class Fusion(torch.nn.Module):
    """
    Prepends variate label embeddings to the input embeddings along the sequence dimension.
    """

    def __init__(self) -> None:
        super().__init__()

    def forward(
        self,
        embeddings: Float[torch.Tensor, "batch variate seq_len embed_dim"],
        variate_label_embeds: Optional[
            Float[torch.Tensor, "batch variate 1 embed_dim"]
        ] = None,
    ) -> Float[torch.Tensor, "batch variate new_seq_len embed_dim"]:

        if variate_label_embeds is None:
            return embeddings

        processed_embeddings = F.normalize(variate_label_embeds, p=2, dim=-1)

        return torch.cat(
            [
                processed_embeddings.to(
                    dtype=embeddings.dtype, device=embeddings.device, non_blocking=True
                ),
                embeddings,
            ],
            dim=2,
        )
