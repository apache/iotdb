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

import io
import unittest

import torch
from torch.nn.utils.rnn import (
    PackedSequence,
    pack_padded_sequence,
    pad_packed_sequence,
    unpack_sequence,
)


def bare_list_annotation():
    values: list = []
    return values


def bare_tuple_annotation():
    values: tuple = ()
    return values


class DependencySecurityTest(unittest.TestCase):
    def test_empty_packed_sequences_are_rejected(self):
        # CVE-2025-2998 / CVE-2025-2999: reject empty batch_sizes before
        # _pad_packed_sequence reads its first element.
        packed = PackedSequence(torch.empty(0, 5), torch.empty(0, dtype=torch.int64))
        for unpack in (pad_packed_sequence, unpack_sequence):
            with self.subTest(unpack=unpack.__name__):
                with self.assertRaisesRegex(
                    RuntimeError, "batch_sizes can not be empty"
                ):
                    unpack(packed)

    def test_valid_packed_sequences_round_trip(self):
        values = torch.arange(24, dtype=torch.float32).reshape(2, 3, 4)
        lengths = torch.tensor([3, 2])
        packed = pack_padded_sequence(values, lengths, batch_first=True)
        padded, actual_lengths = pad_packed_sequence(packed, batch_first=True)
        torch.testing.assert_close(actual_lengths, lengths)
        torch.testing.assert_close(padded[0], values[0])
        torch.testing.assert_close(padded[1, :2], values[1, :2])
        sequences = unpack_sequence(packed)
        self.assertEqual(2, len(sequences))
        torch.testing.assert_close(sequences[0], values[0])
        torch.testing.assert_close(sequences[1], values[1, :2])

    def test_lstm_rejects_inconsistent_gate_weights(self):
        # CVE-2025-3001: all four LSTM gates must have hidden_size rows.
        inputs = torch.zeros(2, 3)
        hidden = (torch.zeros(2, 4), torch.zeros(2, 4))
        weight_ih = torch.zeros(16, 3)
        weight_hh = torch.zeros(16, 4)
        for name, actual_ih, actual_hh in (
            ("weight_ih", torch.zeros(17, 3), weight_hh),
            ("weight_hh", weight_ih, torch.zeros(17, 4)),
        ):
            with self.subTest(weight=name):
                with self.assertRaisesRegex(RuntimeError, name):
                    torch.lstm_cell(inputs, hidden, actual_ih, actual_hh)
        output_hidden, output_cell = torch.lstm_cell(
            inputs, hidden, weight_ih, weight_hh
        )
        torch.testing.assert_close(output_hidden, hidden[0])
        torch.testing.assert_close(output_cell, hidden[1])

    def test_jit_rejects_bare_container_annotations(self):
        # CVE-2025-3000: reject bare container value annotations at compile time.
        for container, function in (
            ("list", bare_list_annotation),
            ("tuple", bare_tuple_annotation),
        ):
            with self.subTest(container=container):
                with self.assertRaisesRegex(
                    RuntimeError,
                    f"Attempted to use {container} without a contained type",
                ):
                    torch.jit.script(function)

    def test_torchscript_model_save_load_preserves_inference(self):
        # AINode loads TorchScript models with torch.jit.load. Exercise the
        # real serialization and inference path without downloading a model.
        model = torch.nn.Linear(3, 2).eval()
        with torch.no_grad():
            model.weight.copy_(torch.arange(6, dtype=torch.float32).reshape(2, 3))
            model.bias.copy_(torch.tensor([1.0, -1.0]))
        inputs = torch.tensor([[1.0, 2.0, 3.0], [3.0, 2.0, 1.0]])
        buffer = io.BytesIO()
        torch.jit.save(torch.jit.script(model), buffer)
        buffer.seek(0)
        restored = torch.jit.load(buffer, map_location="cpu")
        with torch.no_grad():
            torch.testing.assert_close(restored(inputs), model(inputs))


if __name__ == "__main__":
    unittest.main()
