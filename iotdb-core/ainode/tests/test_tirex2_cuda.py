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

"""TiRex-2's recurrence kernels on CUDA, against the PyTorch reference implementations.

The Triton kernels run on GPUs of compute capability 8.0 and newer; these tests compare them with
the PyTorch kernels that TiRex-2 uses everywhere else, kernel by kernel and through the whole
model, on sequence lengths that are and are not multiples of the kernels' chunk sizes. On older
GPUs only the PyTorch kernels on CUDA are compared with the CPU.
"""

import copy
import unittest
from unittest import mock

import torch

from iotdb.ainode.core.model.tirex2.component import kernel
from iotdb.ainode.core.model.tirex2.modeling_tirex2 import TiRex2ForPrediction
from iotdb.ainode.core.model.tirex2.types import TimeseriesType
from tests.test_tirex2 import TINY_CONFIG

HAS_CUDA = torch.cuda.is_available()
HAS_TRITON_GPU = HAS_CUDA and kernel.use_triton(torch.zeros(1, device="cuda"))
NO_TRITON_REASON = (
    "needs CUDA with a GPU of compute capability 8.0 or newer and Triton"
    + (
        f" (this GPU: {torch.cuda.get_device_name()}, "
        f"{'.'.join(map(str, torch.cuda.get_device_capability()))})"
        if HAS_CUDA
        else ""
    )
)

# The dimensions of the published NX-AI/TiRex-2: embedding 512, 4 heads; the mLSTM's query/key
# head dimension is 64 and its value head dimension 128, the sLSTM's head dimension 128.
NH = 4
MLSTM_DHQK, MLSTM_DHHV = 64, 128
SLSTM_HD = 128
# Lengths around the mLSTM's chunk of 64, so that the last chunk is padded or not
SEQUENCE_LENGTHS = (1, 17, 63, 64, 65, 191, 256)


def _model_config(embedding_dim):
    """TINY_CONFIG with another embedding width: at 128 with 2 heads, the mLSTM heads are 32 and 64
    wide and the sLSTM heads 64, enough for the Triton kernels; TINY_CONFIG's own 8-wide mLSTM heads
    are below their minimum of 16 and run on the PyTorch kernels."""
    config = copy.deepcopy(TINY_CONFIG)
    config["embedding_dim"] = embedding_dim
    config["input_ff_dim"] = 2 * embedding_dim
    for template in config["stack_config"]["templates"].values():
        template["time_mixer"]["embedding_dim"] = embedding_dim
        template["variate_mixer"]["embedding_dim"] = embedding_dim
    return config


def _mlstm_inputs(batch, length, device, seed):
    g = torch.Generator().manual_seed(seed)
    q = torch.randn(batch, NH, length, MLSTM_DHQK, generator=g)
    k = torch.randn(batch, NH, length, MLSTM_DHQK, generator=g)
    v = torch.randn(batch, NH, length, MLSTM_DHHV, generator=g)
    i = torch.randn(batch, NH, length, generator=g)
    f = 3.0 + torch.randn(batch, NH, length, generator=g)  # forget gates mostly open
    return [x.to(device) for x in (q, k, v, i, f)]


def _slstm_inputs(batch, length, device, seed, zero_states):
    g = torch.Generator().manual_seed(seed)
    x = torch.randn(batch, length, NH, 4, SLSTM_HD, generator=g)
    states = torch.zeros(batch, NH, 4, SLSTM_HD)
    if not zero_states:
        states = 0.5 * torch.randn(batch, NH, 4, SLSTM_HD, generator=g)
        states[:, :, 2] = states[:, :, 2].abs() + 1.0  # normalizer state n > 0
    # As in the model: recurrent weights in bfloat16, bias and states in float32
    weight = (torch.randn(NH, 4, SLSTM_HD, SLSTM_HD, generator=g) / SLSTM_HD**0.5).to(
        torch.bfloat16
    )
    bias = 0.1 * torch.randn(NH, 4, SLSTM_HD, generator=g)
    return [t.to(device) for t in (x, states, weight, bias)]


def _assert_close(test, actual, expected, tolerance, what):
    """The largest difference, relative to the largest magnitude of the expected values, is within
    tolerance: the kernels' float32 dot products may run in TF32, which rounds each product to about
    three significant digits, so an element-wise relative check would fail on outputs near zero.
    """
    actual, expected = actual.float().cpu(), expected.float().cpu()
    test.assertEqual(actual.shape, expected.shape, what)
    test.assertTrue(torch.isfinite(actual).all(), what)
    scale = max(expected.abs().max().item(), 1e-6)
    error = (actual - expected).abs().max().item() / scale
    test.assertLessEqual(
        error, tolerance, f"{what}: max |difference| / max |expected| = {error:.2e}"
    )


def _forecast(model, contexts, output_length, pytorch_kernels=False):
    """The model's forecast, with its own choice of kernels or with the PyTorch kernels forced."""
    with torch.no_grad():
        if not pytorch_kernels:
            return model.predict(contexts, output_length)
        with mock.patch.object(kernel, "use_triton", return_value=False):
            return model.predict(contexts, output_length)


def _contexts(device):
    """Contexts whose lengths are and are not multiples of the patch size, one with missing values."""
    g = torch.Generator().manual_seed(7)
    walk = lambda *shape: torch.randn(*shape, generator=g).cumsum(-1)
    with_gaps = walk(2, 77)
    with_gaps[:, :9] = float("nan")  # missing values at the start
    with_gaps[1, 40:43] = float("nan")
    return [
        TimeseriesType(
            target=t.to(device), past_covariates=None, future_covariates=None
        )
        for t in (walk(1, 64), walk(3, 37), with_gaps, walk(1, 5))
    ]


@unittest.skipUnless(HAS_TRITON_GPU, NO_TRITON_REASON)
class TiRex2TritonKernelTest(unittest.TestCase):
    """The Triton kernels against the PyTorch kernels, on the same GPU."""

    def test_mlstm_matches_native(self):
        from iotdb.ainode.core.model.tirex2.component.kernel.mlstm_native import (
            mlstm_chunkwise_native,
        )
        from iotdb.ainode.core.model.tirex2.component.kernel.mlstm_triton import (
            mlstm_chunkwise_triton,
        )

        for batch in (1, 3):
            for length in SEQUENCE_LENGTHS:
                inputs = _mlstm_inputs(
                    batch, length, "cuda", seed=batch * 1000 + length
                )
                with self.subTest(batch=batch, length=length), torch.no_grad():
                    expected = mlstm_chunkwise_native(*inputs)
                    actual = mlstm_chunkwise_triton(*inputs)
                    _assert_close(self, actual, expected, 1e-2, "mLSTM, float32")

    def test_mlstm_under_bfloat16_autocast(self):
        from iotdb.ainode.core.model.tirex2.component.kernel.mlstm_native import (
            mlstm_chunkwise_native,
        )
        from iotdb.ainode.core.model.tirex2.component.kernel.mlstm_triton import (
            mlstm_chunkwise_triton,
        )

        for length in (63, 65, 256):
            inputs = _mlstm_inputs(2, length, "cuda", seed=length)
            with self.subTest(length=length), torch.no_grad():
                expected = mlstm_chunkwise_native(*inputs)
                with torch.autocast("cuda", dtype=torch.bfloat16):
                    actual = mlstm_chunkwise_triton(*inputs)
                self.assertEqual(actual.dtype, torch.bfloat16)
                _assert_close(self, actual, expected, 1e-1, "mLSTM, bfloat16")

    def test_slstm_matches_torch(self):
        from iotdb.ainode.core.model.tirex2.component.kernel.slstm_torch import (
            slstm_torch,
        )
        from iotdb.ainode.core.model.tirex2.component.kernel.slstm_triton import (
            slstm_triton_fw,
        )

        for batch in (1, 3):
            for length in (1, 7, 64, 257):
                for zero_states in (True, False):
                    inputs = _slstm_inputs(
                        batch,
                        length,
                        "cuda",
                        seed=batch * 1000 + length,
                        zero_states=zero_states,
                    )
                    with self.subTest(
                        batch=batch, length=length, zero_states=zero_states
                    ), torch.no_grad():
                        expected_out, expected_states = slstm_torch(*inputs)
                        actual_out, actual_states = slstm_triton_fw(*inputs)
                        _assert_close(
                            self, actual_out, expected_out, 5e-3, "sLSTM output"
                        )
                        _assert_close(
                            self, actual_states, expected_states, 5e-3, "sLSTM states"
                        )

    def test_model_forecasts_match_pytorch_kernels(self):
        torch.manual_seed(0)
        model = TiRex2ForPrediction(**_model_config(128)).eval().to("cuda")
        x = torch.zeros(1, device="cuda")
        self.assertTrue(
            kernel.use_triton(x, 32, 64)
        )  # the model's head widths take Triton
        contexts = _contexts("cuda")
        expected = _forecast(model, contexts, 24, pytorch_kernels=True)
        actual = _forecast(model, contexts, 24)
        for n, (a, e) in enumerate(zip(actual, expected)):
            with self.subTest(context=n):
                _assert_close(self, a, e, 1e-2, f"forecast of context {n}")

    def test_heads_narrower_than_16_run_on_pytorch_kernels(self):
        # tl.dot needs operands at least 16 wide; TINY_CONFIG's mLSTM heads are 8 wide
        torch.manual_seed(0)
        model = TiRex2ForPrediction(**copy.deepcopy(TINY_CONFIG)).eval().to("cuda")
        contexts = _contexts("cuda")
        expected = _forecast(model, contexts, 24, pytorch_kernels=True)
        actual = _forecast(model, contexts, 24)
        for n, (a, e) in enumerate(zip(actual, expected)):
            with self.subTest(context=n):
                _assert_close(self, a, e, 1e-2, f"forecast of context {n}")


@unittest.skipUnless(HAS_CUDA, "needs CUDA")
class TiRex2CudaTest(unittest.TestCase):
    """The PyTorch kernels on CUDA against the CPU, on any GPU."""

    def test_model_forecasts_match_cpu(self):
        torch.manual_seed(0)
        model = TiRex2ForPrediction(**_model_config(128)).eval()
        expected = _forecast(model, _contexts("cpu"), 24, pytorch_kernels=True)
        actual = _forecast(
            model.to("cuda"), _contexts("cuda"), 24, pytorch_kernels=True
        )
        for n, (a, e) in enumerate(zip(actual, expected)):
            with self.subTest(context=n):
                _assert_close(self, a, e, 1e-3, f"forecast of context {n}")


if __name__ == "__main__":
    unittest.main()
