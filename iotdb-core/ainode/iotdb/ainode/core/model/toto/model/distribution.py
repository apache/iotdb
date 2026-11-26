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

from abc import ABC

import torch
import torch.nn.functional as F
from torch.distributions import TransformedDistribution
from torch.distributions.transforms import AffineTransform


class AffineTransformed(TransformedDistribution):
    """
    A thin wrapper around TransformedDistribution with an AffineTransform,
    replacing the gluonts.torch.distributions.AffineTransformed dependency.
    Provides the same interface: mean, variance, sample(), log_prob().
    """

    def __init__(self, base_distribution, loc=0.0, scale=1.0):
        super().__init__(base_distribution, AffineTransform(loc=loc, scale=scale))

    @property
    def mean(self):
        # mean(aX + b) = a * mean(X) + b
        loc = self.transforms[0].loc
        scale = self.transforms[0].scale
        return loc + scale * self.base_dist.mean

    # Note: Do NOT override sample() here. TransformedDistribution.sample() correctly
    # calls base_dist.sample() (not rsample), which works for non-reparameterizable
    # distributions like MixtureSameFamily.


class DistributionOutput(ABC, torch.nn.Module):
    pass


class StudentTOutput(DistributionOutput):
    def __init__(self, embed_dim):
        super().__init__()
        self.embed_dim = embed_dim
        self.df = torch.nn.Linear(embed_dim, 1)
        self.loc_proj = torch.nn.Linear(embed_dim, 1)
        self.scale_proj = torch.nn.Linear(embed_dim, 1)

    def forward(self, inputs, loc=None, scale=None):
        eps = torch.finfo(inputs.dtype).eps
        df = 2.0 + F.softplus(self.df(inputs)).clamp_min(eps).squeeze(-1)
        base_loc = self.loc_proj(inputs).squeeze(-1)
        base_scale = F.softplus(self.scale_proj(inputs)).clamp_min(eps).squeeze(-1)

        base_dist = torch.distributions.StudentT(
            df, base_loc, base_scale, validate_args=False
        )

        if loc is not None and scale is not None:
            return AffineTransformed(base_dist, loc=loc, scale=scale)
        return base_dist


class MixtureOfStudentTsOutput(DistributionOutput):
    def __init__(self, embed_dim, k_components):
        super().__init__()
        self.embed_dim = embed_dim
        self.k_components = k_components

        self.df = torch.nn.Linear(embed_dim, k_components)
        self.loc_proj = torch.nn.Linear(embed_dim, k_components)
        self.scale_proj = torch.nn.Linear(embed_dim, k_components)
        self.mixture_weights = torch.nn.Linear(embed_dim, k_components)

    def forward(self, inputs, loc=None, scale=None):
        df = 2.0 + F.softplus(self.df(inputs)).clamp_min(torch.finfo(inputs.dtype).eps)
        component_loc = self.loc_proj(inputs)
        component_scale = F.softplus(self.scale_proj(inputs)).clamp_min(
            torch.finfo(inputs.dtype).eps
        )
        logits = self.mixture_weights(inputs)
        probs = F.softmax(logits, dim=-1)
        components = torch.distributions.StudentT(
            df, component_loc, component_scale, validate_args=False
        )
        mixture_distribution = torch.distributions.Categorical(probs=probs)

        return torch.distributions.MixtureSameFamily(mixture_distribution, components)


DISTRIBUTION_CLASSES_LOOKUP = {
    "<class 'model.distribution.StudentTOutput'>": StudentTOutput,
    "<class 'model.distribution.MixtureOfStudentTsOutput'>": MixtureOfStudentTsOutput,
    # Short-form aliases for convenience
    "student_t": StudentTOutput,
    "student_t_mixture": MixtureOfStudentTsOutput,
}
