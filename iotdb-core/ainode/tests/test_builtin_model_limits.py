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
from unittest import mock

import torch

from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.manager.inference_manager import InferenceManager
from iotdb.ainode.core.model.model_info import BUILTIN_HF_TRANSFORMERS_MODEL_MAP
from iotdb.ainode.core.util.serde import convert_tensor_to_tsblock
from iotdb.thrift.ainode.ttypes import TForecastReq


class BuiltinModelLimitsTest(unittest.TestCase):
    """output_length is checked against a model's own limit before pool dispatch."""

    def _manager(self, model_info):
        manager = object.__new__(InferenceManager)
        manager._model_manager = mock.Mock()
        manager._model_manager.is_model_registered.return_value = True
        manager._model_manager.get_model_info.return_value = model_info
        return manager

    def test_model_limit_lowers_the_ainode_limit(self):
        model_info = BUILTIN_HF_TRANSFORMERS_MODEL_MAP["tirex2"]
        self.assertEqual(model_info.max_output_length, 320)
        self.assertEqual(self._manager(model_info)._max_output_length("tirex2"), 320)

    def test_models_without_a_limit_keep_the_ainode_limit(self):
        model_info = BUILTIN_HF_TRANSFORMERS_MODEL_MAP["chronos2"]
        self.assertIsNone(model_info.max_output_length)
        self.assertGreater(
            self._manager(model_info)._max_output_length("chronos2"), 320
        )

    def test_loaded_model_rejects_long_horizon_and_serves_next_request(self):
        # With tirex2 loaded, a request above its horizon must fail without reaching the
        # inference pool, and the next valid request must still be served by the pool.
        manager = self._manager(BUILTIN_HF_TRANSFORMERS_MODEL_MAP["tirex2"])
        manager._pool_controller = mock.Mock()
        manager._pool_controller.has_running_pools.return_value = True
        dispatched = []

        def process_request(request):
            dispatched.append(request)
            return [torch.zeros(1, request.output_length)]

        manager._process_request = process_request
        input_data = convert_tensor_to_tsblock(torch.randn(1, 64))

        rejected = manager.forecast(
            TForecastReq(modelId="tirex2", inputData=input_data, outputLength=400)
        )
        self.assertEqual(rejected.status.code, TSStatusCode.AINODE_INTERNAL_ERROR.value)
        self.assertIn("between 1 and 320, got 400", rejected.status.message)
        self.assertEqual(dispatched, [])

        served = manager.forecast(
            TForecastReq(modelId="tirex2", inputData=input_data, outputLength=24)
        )
        self.assertEqual(served.status.code, TSStatusCode.SUCCESS_STATUS.value)
        self.assertEqual([r.output_length for r in dispatched], [24])


if __name__ == "__main__":
    unittest.main()
