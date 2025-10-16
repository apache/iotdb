1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1from collections import OrderedDict
1
1import torch
1import torch.nn as nn
1import torch.nn.functional as F
1
1
1class ClampedGELU(nn.Module):
1    def __init__(self, min_val=-10, max_val=10):
1        super().__init__()
1        self.act = nn.GELU()
1        self.min_val = min_val
1        self.max_val = max_val
1
1    def forward(self, x):
1        return torch.clamp(self.act(x), self.min_val, self.max_val)
1
1
1class ClassInstantier(OrderedDict):
1    def __getitem__(self, key):
1        content = super().__getitem__(key)
1        cls, kwargs = content if isinstance(content, tuple) else (content, {})
1        return cls(**kwargs)
1
1
1ACT2CLS = {
1    "gelu": nn.GELU,
1    "gelu_10": (ClampedGELU, {"min": -10, "max": 10}),
1    "leaky_relu": nn.LeakyReLU,
1    "relu": nn.ReLU,
1    "relu6": nn.ReLU6,
1    "sigmoid": nn.Sigmoid,
1    "silu": nn.SiLU,
1    "swish": nn.SiLU,
1    "tanh": nn.Tanh,
1    "prelu": nn.PReLU,
1}
1ACT2FN = ClassInstantier(ACT2CLS)
1