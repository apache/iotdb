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
from iotdb.ainode.core.model.tirex2.component import kernel
from iotdb.ainode.core.model.tirex2.modeling_tirex2 import TiRex2ForPrediction
from iotdb.ainode.core.model.tirex2.pipeline_tirex2 import TiRex2Pipeline

QUANTILES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]


def _template(model_type):
    return {
        "time_mixer": {"model_type": model_type, "embedding_dim": 32, "num_heads": 2},
        "variate_mixer": {"embedding_dim": 32, "num_heads": 2},
        "dropout": 0.0,
        "eps": 1e-7,
    }


# A small TiRex-2 with the structure of NX-AI/TiRex-2: alternating mLSTM and sLSTM blocks
TINY_CONFIG = {
    "num_blocks": 2,
    "embedding_dim": 32,
    "input_patch_size": 8,
    "output_patch_size": 8,
    "input_ff_dim": 64,
    "h_expand": 2,
    "context_len": 64,
    "future_len": 32,
    "dropout": 0.0,
    "quantiles": QUANTILES,
    "act_func": "torch.nn.SiLU",
    "use_qk_norm": True,
    "tokenizer_cfg": {
        "input_patch_size": 8,
        "output_patch_size": 8,
        "input_patch_stride": 8,
    },
    "scaler_cfg": {"use_arcsinh": True, "binaryaware": True},
    "stack_out_norm_config": {"eps": 1e-7},
    "tta_diff": True,
    "stack_config": {
        "templates": {
            "mlstm_attn": _template("bi-mlstm"),
            "slstm_attn": _template("bi-slstm"),
        },
        "recipe": ["mlstm_attn", "slstm_attn"],
    },
}


def _published_layout(state_dict):
    """The state dict in the layout of the published checkpoint: one weight per gate."""
    state_dict = dict(state_dict)
    for key in [k for k in state_dict if k.endswith("fwd_cell.gate_proj.weight")]:
        prefix = key[: -len("gate_proj.weight")]
        weights = state_dict.pop(key)
        for i, gate in enumerate(("fgate", "igate", "zgate", "ogate")):
            state_dict[f"{prefix}{gate}.weight"] = weights[i].clone()
    for key in [k for k in state_dict if k.endswith("fwd_cell.in_proj.weight")]:
        prefix = key[: -len("in_proj.weight")]
        weights = state_dict.pop(key)
        biases = state_dict.pop(f"{prefix}in_proj.bias")
        qk_dim, v_dim, heads = 16, 32, 2
        split = (qk_dim, qk_dim, v_dim, v_dim, heads, heads)
        names = ("q", "k", "v", "ogate_preact", "igate_preact", "fgate_preact")
        for name, w, b in zip(names, weights.split(split), biases.split(split)):
            state_dict[f"{prefix}{name}.weight"] = w.clone()
            if name in ("igate_preact", "fgate_preact"):
                state_dict[f"{prefix}{name}.bias"] = b.clone()
    return state_dict


class TiRex2Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.manual_seed(0)
        cls.model = TiRex2ForPrediction(**copy.deepcopy(TINY_CONFIG)).eval()
        cls.checkpoint = tempfile.TemporaryDirectory()
        with open(os.path.join(cls.checkpoint.name, "config.json"), "w") as f:
            json.dump(TINY_CONFIG, f)
        save_file(
            _published_layout(cls.model.state_dict()),
            os.path.join(cls.checkpoint.name, "model.safetensors"),
        )

    @classmethod
    def tearDownClass(cls):
        cls.checkpoint.cleanup()

    def load_pipeline(self):
        model_info = copy.copy(BUILTIN_HF_TRANSFORMERS_MODEL_MAP["tirex2"])
        with mock.patch(
            "iotdb.ainode.core.model.model_loader.get_model_path",
            return_value=self.checkpoint.name,
        ), mock.patch(
            "iotdb.ainode.core.model.utils.get_model_path",
            return_value=self.checkpoint.name,
        ):
            return TiRex2Pipeline(model_info, device=torch.device("cpu"))

    def run_pipeline(self, pipeline, inputs, output_length):
        kwargs = {"output_length": output_length}
        series = pipeline.preprocess(inputs, **kwargs)
        return pipeline.postprocess(pipeline.forecast(series, **kwargs), **kwargs)

    def test_published_checkpoint_layout_loads(self):
        loaded = TiRex2ForPrediction.from_pretrained(self.checkpoint.name)
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
            state_dict = _published_layout(self.model.state_dict())
            state_dict.pop("output_patch_embedding.output_layer.weight")
            save_file(state_dict, os.path.join(directory, "model.safetensors"))
            with self.assertRaises(RuntimeError):
                TiRex2ForPrediction.from_pretrained(directory)

    def test_builtin_pipeline_forecasts_median(self):
        pipeline = self.load_pipeline()
        self.assertIsInstance(pipeline.model, TiRex2ForPrediction)
        torch.manual_seed(1)
        inputs = [
            {"targets": torch.randn(100).cumsum(0)},
            {"targets": torch.randn(3, 40).cumsum(-1)},
        ]
        outputs = self.run_pipeline(pipeline, copy.deepcopy(inputs), 24)
        self.assertEqual([tuple(o.shape) for o in outputs], [(1, 24), (3, 24)])
        for output in outputs:
            self.assertEqual(output.dtype, torch.float32)
            self.assertTrue(torch.isfinite(output).all())

        # the median quantile of the model's own forecast
        series = pipeline.preprocess(copy.deepcopy(inputs), output_length=24)
        quantiles = self.model.predict(series, 24)
        for output, forecast in zip(outputs, quantiles):
            torch.testing.assert_close(output, forecast[:, QUANTILES.index(0.5), :])

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
                "future_covariates": {"known": torch.randn(16)},
            }
        ]
        series = pipeline.preprocess(copy.deepcopy(inputs), output_length=16)
        self.assertEqual(tuple(series[0].target.shape), (2, 60))
        self.assertEqual(tuple(series[0].past_covariates.shape), (1, 60))
        self.assertEqual(tuple(series[0].future_covariates.shape), (1, 76))
        torch.testing.assert_close(
            series[0].future_covariates[0],
            torch.cat(
                [
                    inputs[0]["past_covariates"]["known"],
                    inputs[0]["future_covariates"]["known"],
                ]
            ),
        )
        outputs = self.run_pipeline(pipeline, copy.deepcopy(inputs), 16)
        self.assertEqual(tuple(outputs[0].shape), (2, 16))

    def test_output_length_above_model_horizon_is_rejected(self):
        pipeline = self.load_pipeline()
        inputs = [{"targets": torch.randn(50)}]
        self.run_pipeline(pipeline, copy.deepcopy(inputs), TINY_CONFIG["future_len"])
        with self.assertRaises(ValueError):
            self.run_pipeline(
                pipeline, copy.deepcopy(inputs), TINY_CONFIG["future_len"] + 1
            )

    def test_activation_must_be_a_torch_nn_module(self):
        for act_func in ("os.system", "torch.nn.functional.silu", "Linear.x"):
            config = copy.deepcopy(TINY_CONFIG)
            config["act_func"] = act_func
            with self.subTest(act_func=act_func), self.assertRaises(ValueError):
                TiRex2ForPrediction(**config)

    def test_cpu_inputs_use_pytorch_kernels(self):
        self.assertFalse(kernel.use_triton(torch.zeros(1)))


if __name__ == "__main__":
    unittest.main()
