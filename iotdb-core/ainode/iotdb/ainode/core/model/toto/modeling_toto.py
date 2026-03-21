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

from iotdb.ainode.core.log import Logger

logger = Logger()


class TotoForPrediction(torch.nn.Module):
    """
    Wrapper around the Toto model for AINode integration.

    Toto (Time Series Optimized Transformer for Observability) is a 151M parameter
    foundation model for multivariate time series forecasting. This wrapper delegates
    model loading to the ``toto-ts`` package while providing a compatible interface
    for AINode's model loading mechanism.

    The underlying Toto model uses ``huggingface_hub.ModelHubMixin`` for ``from_pretrained``
    support, which differs from the standard ``transformers.PreTrainedModel`` pattern.
    This wrapper bridges that gap.

    Reference: https://huggingface.co/Datadog/Toto-Open-Base-1.0
    """

    def __init__(self, toto_model):
        """
        Initialize the wrapper with a loaded Toto model instance.

        Args:
            toto_model: A ``toto.model.toto.Toto`` instance.
        """
        super().__init__()
        self.toto = toto_model

    @classmethod
    def from_pretrained(cls, pretrained_model_name_or_path, **kwargs):
        """
        Load a Toto model from a local directory or HuggingFace Hub repository.

        This delegates to ``toto.model.toto.Toto.from_pretrained()`` which uses
        ``ModelHubMixin`` to load the model weights and configuration.

        Args:
            pretrained_model_name_or_path (str): Path to a local directory containing
                ``config.json`` and ``model.safetensors``, or a HuggingFace Hub repo ID
                (e.g., ``Datadog/Toto-Open-Base-1.0``).
            **kwargs: Additional keyword arguments passed to the underlying loader.

        Returns:
            TotoForPrediction: A wrapper instance containing the loaded Toto model.
        """
        from toto.model.toto import Toto

        toto_model = Toto.from_pretrained(pretrained_model_name_or_path, **kwargs)
        logger.info(f"Loaded Toto model from {pretrained_model_name_or_path}")
        return cls(toto_model)

    @classmethod
    def from_config(cls, config):
        """
        Create a Toto model from a configuration (for training from scratch).

        Args:
            config: A ``TotoConfig`` or compatible configuration object.

        Returns:
            TotoForPrediction: A wrapper instance containing a newly initialized Toto model.
        """
        from toto.model.toto import Toto

        toto_model = Toto(
            patch_size=getattr(config, "patch_size", 32),
            stride=getattr(config, "stride", 32),
            embed_dim=getattr(config, "embed_dim", 1024),
            num_layers=getattr(config, "num_layers", 18),
            num_heads=getattr(config, "num_heads", 16),
            mlp_hidden_dim=getattr(config, "mlp_hidden_dim", 2816),
            dropout=getattr(config, "dropout", 0.0),
            spacewise_every_n_layers=getattr(config, "spacewise_every_n_layers", 3),
            scaler_cls=getattr(config, "scaler_cls", "per_variate_causal"),
            output_distribution_classes=getattr(
                config, "output_distribution_classes", ["student_t_mixture"]
            ),
            spacewise_first=getattr(config, "spacewise_first", True),
            use_memory_efficient_attention=getattr(
                config, "use_memory_efficient_attention", True
            ),
            stabilize_with_global=getattr(config, "stabilize_with_global", True),
            scale_factor_exponent=getattr(config, "scale_factor_exponent", 10.0),
        )
        return cls(toto_model)

    @property
    def backbone(self):
        """
        Access the underlying TotoBackbone model used for inference.

        Returns:
            The ``TotoBackbone`` instance from the Toto model.
        """
        return self.toto.model

    @property
    def device(self):
        """
        Get the device of the model parameters.

        Returns:
            torch.device: The device where the model parameters reside.
        """
        return self.toto.device
