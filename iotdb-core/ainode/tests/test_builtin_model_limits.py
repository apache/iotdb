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

from iotdb.ainode.core.manager.inference_manager import InferenceManager
from iotdb.ainode.core.model.model_info import BUILTIN_HF_TRANSFORMERS_MODEL_MAP


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


if __name__ == "__main__":
    unittest.main()
