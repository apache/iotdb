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

import unittest

import torch

from iotdb.ainode.core.model.sundial.flow_loss import (
    ConditionalOutputProjection,
    ConditionalResidualBlock,
    FlowLoss,
    SimpleMLPAdaLN,
)


class SundialFlowLossTest(unittest.TestCase):
    def create_network(self):
        return SimpleMLPAdaLN(
            in_channels=4,
            model_channels=8,
            out_channels=4,
            z_channels=6,
            num_res_blocks=2,
        )

    def test_checkpoint_parameter_contract(self):
        network = self.create_network()
        expected_shapes = {
            "time_embed.mlp.0.weight": (8, 256),
            "time_embed.mlp.0.bias": (8,),
            "time_embed.mlp.2.weight": (8, 8),
            "time_embed.mlp.2.bias": (8,),
            "cond_embed.weight": (8, 6),
            "cond_embed.bias": (8,),
            "input_proj.weight": (8, 4),
            "input_proj.bias": (8,),
            "res_blocks.0.in_ln.weight": (8,),
            "res_blocks.0.in_ln.bias": (8,),
            "res_blocks.0.mlp.0.weight": (8, 8),
            "res_blocks.0.mlp.0.bias": (8,),
            "res_blocks.0.mlp.2.weight": (8, 8),
            "res_blocks.0.mlp.2.bias": (8,),
            "res_blocks.0.adaLN_modulation.1.weight": (24, 8),
            "res_blocks.0.adaLN_modulation.1.bias": (24,),
            "res_blocks.1.in_ln.weight": (8,),
            "res_blocks.1.in_ln.bias": (8,),
            "res_blocks.1.mlp.0.weight": (8, 8),
            "res_blocks.1.mlp.0.bias": (8,),
            "res_blocks.1.mlp.2.weight": (8, 8),
            "res_blocks.1.mlp.2.bias": (8,),
            "res_blocks.1.adaLN_modulation.1.weight": (24, 8),
            "res_blocks.1.adaLN_modulation.1.bias": (24,),
            "final_layer.linear.weight": (4, 8),
            "final_layer.linear.bias": (4,),
            "final_layer.adaLN_modulation.1.weight": (16, 8),
            "final_layer.adaLN_modulation.1.bias": (16,),
        }

        actual_shapes = {
            key: tuple(value.shape) for key, value in network.state_dict().items()
        }
        self.assertEqual(expected_shapes, actual_shapes)

        restored = self.create_network()
        result = restored.load_state_dict(network.state_dict(), strict=True)
        self.assertEqual([], result.missing_keys)
        self.assertEqual([], result.unexpected_keys)

    def test_conditioned_layers_propagate_gradients(self):
        features = torch.randn(3, 8, requires_grad=True)
        condition = torch.randn(3, 8, requires_grad=True)
        block = ConditionalResidualBlock(8)
        projection = ConditionalOutputProjection(8, 4)

        output = projection(block(features, condition), condition)

        self.assertEqual((3, 4), tuple(output.shape))
        output.square().mean().backward()
        self.assertIsNotNone(features.grad)
        self.assertIsNotNone(condition.grad)

    def test_network_starts_with_zero_output(self):
        network = self.create_network()

        output = network(
            torch.randn(3, 4),
            torch.tensor([0.0, 500.0, 999.0]),
            torch.randn(3, 6),
        )

        torch.testing.assert_close(output, torch.zeros_like(output))

    def test_sample_shape(self):
        flow_loss = FlowLoss(
            target_channels=4,
            z_channels=6,
            depth=2,
            width=8,
            num_sampling_steps=2,
        )

        samples = flow_loss.sample(torch.randn(2, 6), num_samples=3)

        self.assertEqual((2, 3, 4), tuple(samples.shape))


if __name__ == "__main__":
    unittest.main()
