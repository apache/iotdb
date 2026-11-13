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

import copy
import json
import os
import tempfile
import unittest
from unittest import mock

import torch
from safetensors.torch import save_file

from iotdb.ainode.core.model.model_info import BUILTIN_HF_TRANSFORMERS_MODEL_MAP
from iotdb.ainode.core.model.timestral.forecaster import TimestralForecaster
from iotdb.ainode.core.model.timestral.modeling_timestral import (
    TimestralForPrediction,
)
from iotdb.ainode.core.model.timestral.pipeline_timestral import TimestralPipeline

QUANTILES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

# A small Timestral with the structure of the published checkpoints; one decode block is
# block_size * patch_size = 16 steps
TINY_CONFIG = {
    "patch_size": 4,
    "d_model": 16,
    "num_heads": 2,
    "mlp_hidden_dim": 32,
    "block_schedule": ["time", "time_variate"],
    "quantile_knots": QUANTILES,
    "block_size": 4,
    "context_length": 256,
    "rope_theta": 10000.0,
    "rope_partial_factor": 0.5,
    "norm_eps": 1e-06,
    "attn_bias": False,
    "mlp_bias": False,
    "time_attn_gate": "sdpa_sigmoid",
    "variate_attn_gate": "sdpa_sigmoid",
    "minimum_scale": 1e-06,
    "use_asinh": True,
    "stabilize_with_global": False,
    "covariate_enabled": True,
    "split_context_horizon_attn": True,
    "attend_missing_patches": True,
    "quantile_width_factor": 3.0,
}


class TimestralTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.manual_seed(0)
        cls.model = TimestralForPrediction(**copy.deepcopy(TINY_CONFIG)).eval()
        # the zero-initialized forecast query would make every horizon step alike
        with torch.no_grad():
            cls.model.representation.forecast_query.normal_()
        cls.checkpoint = tempfile.TemporaryDirectory()
        with open(os.path.join(cls.checkpoint.name, "config.json"), "w") as f:
            json.dump(TINY_CONFIG, f)
        save_file(
            cls.model.state_dict(),
            os.path.join(cls.checkpoint.name, "model.safetensors"),
        )

    @classmethod
    def tearDownClass(cls):
        cls.checkpoint.cleanup()

    def load_pipeline(self):
        model_info = copy.copy(BUILTIN_HF_TRANSFORMERS_MODEL_MAP["timestral"])
        with mock.patch(
            "iotdb.ainode.core.model.model_loader.get_model_path",
            return_value=self.checkpoint.name,
        ), mock.patch(
            "iotdb.ainode.core.model.utils.get_model_path",
            return_value=self.checkpoint.name,
        ):
            return TimestralPipeline(model_info, device=torch.device("cpu"))

    def run_pipeline(self, pipeline, inputs, output_length):
        kwargs = {"output_length": output_length}
        series = pipeline.preprocess(inputs, **kwargs)
        return pipeline.postprocess(pipeline.forecast(series, **kwargs), **kwargs)

    def test_checkpoint_loads(self):
        loaded = TimestralForPrediction.from_pretrained(self.checkpoint.name)
        self.assertFalse(loaded.training)
        expected = self.model.state_dict()
        actual = loaded.state_dict()
        self.assertEqual(set(expected), set(actual))
        for key in expected:
            torch.testing.assert_close(actual[key], expected[key], rtol=0, atol=0)

    def test_checkpoint_with_missing_weight_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "config.json"), "w") as f:
                json.dump(TINY_CONFIG, f)
            state_dict = dict(self.model.state_dict())
            state_dict.pop("head.projection.output_layer.weight")
            save_file(state_dict, os.path.join(directory, "model.safetensors"))
            with self.assertRaises(RuntimeError):
                TimestralForPrediction.from_pretrained(directory)

    def test_unknown_config_key_is_rejected(self):
        with self.assertRaises(ValueError):
            TimestralForPrediction(**TINY_CONFIG, unknown_key=1)

    def test_rotary_frequencies_follow_the_model(self):
        model = TimestralForPrediction(**copy.deepcopy(TINY_CONFIG))
        expected = model.transformer.rope.inv_freq.clone()
        model.double()
        self.assertEqual(model.transformer.rope.inv_freq.dtype, torch.float32)
        torch.testing.assert_close(model.transformer.rope.inv_freq, expected)

    def test_builtin_pipeline_forecasts_median(self):
        pipeline = self.load_pipeline()
        self.assertIsInstance(pipeline.model, TimestralForPrediction)
        torch.manual_seed(1)
        inputs = [
            {"targets": torch.randn(100).cumsum(0)},
            {"targets": torch.randn(3, 40).cumsum(-1)},
            {"targets": torch.randn(100).cumsum(0)},
        ]
        # longer than one decode block
        outputs = self.run_pipeline(pipeline, copy.deepcopy(inputs), 40)
        self.assertEqual([tuple(o.shape) for o in outputs], [(1, 40), (3, 40), (1, 40)])
        forecaster = TimestralForecaster(self.model)
        for output, item in zip(outputs, inputs):
            self.assertEqual(output.dtype, torch.float32)
            self.assertFalse(output.is_inference())
            targets = item["targets"].reshape(-1, item["targets"].shape[-1])
            expected = forecaster.forecast(targets.unsqueeze(0), 40).median[0]
            torch.testing.assert_close(output, expected)

    def test_covariates(self):
        pipeline = self.load_pipeline()
        torch.manual_seed(2)
        inputs = [
            {
                "targets": torch.randn(2, 60),
                "past_covariates": {
                    "past_only": torch.randn(60),
                    "known": torch.randn(60),
                },
                "future_covariates": {"known": torch.randn(24)},
            }
        ]
        series = pipeline.preprocess(copy.deepcopy(inputs), output_length=24)
        self.assertEqual(tuple(series[0]["context"].shape), (1, 2, 60))
        self.assertEqual(tuple(series[0]["past_covariates"].shape), (1, 1, 60))
        self.assertEqual(tuple(series[0]["known_covariates"].shape), (1, 1, 84))
        torch.testing.assert_close(
            series[0]["known_covariates"][0, 0],
            torch.cat(
                [
                    inputs[0]["past_covariates"]["known"],
                    inputs[0]["future_covariates"]["known"],
                ]
            ),
        )
        outputs = self.run_pipeline(pipeline, copy.deepcopy(inputs), 24)
        self.assertEqual(tuple(outputs[0].shape), (2, 24))
        self.assertTrue(torch.isfinite(outputs[0]).all())
        without = self.run_pipeline(
            pipeline, [{"targets": inputs[0]["targets"].clone()}], 24
        )
        self.assertFalse(torch.equal(outputs[0], without[0]))


if __name__ == "__main__":
    unittest.main()
