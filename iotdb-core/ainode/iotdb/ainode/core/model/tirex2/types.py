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
# Copyright (c) NXAI GmbH.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass

import torch


@dataclass
class TimeseriesType:
    """A single (possibly multivariate) series with optional covariates, as passed to
    :meth:`TiRex2.predict`.

    Parameters
    ----------
    target : torch.Tensor
        Target history, shape ``[V_t, T]`` (``V_t`` target variates, context length ``T``).
        A univariate series is still 2D, with ``V_t == 1``.
    past_covariates : torch.Tensor or None
        Covariates known only up to the current time, shape ``[V_p, T]``, matching the
        target's context length. ``None`` if there are no past covariates.
    future_covariates : torch.Tensor or None
        Covariates known ahead of time for the whole forecast horizon, shape
        ``[V_f, >=T+H]`` (``H`` is the requested ``prediction_length``); extra trailing
        steps beyond ``T+H`` are ignored. ``None`` if there are no future covariates.

    Examples
    --------
    >>> import torch
    >>> from iotdb.ainode.core.model.tirex2.types import TimeseriesType
    >>> ts = TimeseriesType(
    ...     target=torch.randn(1, 128),
    ...     past_covariates=None,
    ...     future_covariates=None,
    ... )
    >>> ts.past_length
    128
    """

    target: torch.Tensor  # [V_t, T]
    past_covariates: torch.Tensor | None  # [V_p, T]
    future_covariates: (
        torch.Tensor | None
    )  # [V_f, >=T+H]; extra future steps are ignored

    @property
    def n_past_covariates(self) -> int:
        return 0 if self.past_covariates is None else len(self.past_covariates)

    @property
    def n_future_covariates(self) -> int:
        return 0 if self.future_covariates is None else len(self.future_covariates)

    @property
    def past_length(self) -> int:
        return self.target.shape[-1]

    @property
    def future_length(self) -> int:
        return (
            0
            if self.future_covariates is None
            else self.future_covariates.shape[-1] - self.past_length
        )
