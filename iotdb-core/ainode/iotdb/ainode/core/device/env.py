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
from dataclasses import dataclass


@dataclass(frozen=True)
class DistEnv:
    rank: int
    local_rank: int
    world_size: int


def read_dist_env() -> DistEnv:
    # torchrun:
    rank = int(os.environ.get("RANK", "0"))
    world_size = int(os.environ.get("WORLD_SIZE", "1"))

    # torchrun provides LOCAL_RANK; slurm often provides SLURM_LOCALID
    local_rank = os.environ.get("LOCAL_RANK", os.environ.get("SLURM_LOCALID", "0"))
    local_rank = int(local_rank)

    return DistEnv(rank=rank, local_rank=local_rank, world_size=world_size)
