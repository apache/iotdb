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

import os
import shutil
import tempfile
import unittest
from unittest import mock

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.model.model_constants import ModelStates
from iotdb.ainode.core.model.model_info import BUILTIN_HF_TRANSFORMERS_MODEL_MAP
from iotdb.ainode.core.model.model_storage import ModelStorage
from iotdb.ainode.core.rpc.handler import AINodeRPCServiceHandler


class BuiltinModelDownloadTest(unittest.TestCase):
    """Timestral's weights are downloaded when the model is loaded, not at start."""

    def setUp(self):
        self.cwd = os.getcwd()
        self.work = tempfile.mkdtemp()
        os.chdir(self.work)
        self.model_dir = os.path.join(
            self.work,
            AINodeDescriptor().get_config().get_ain_models_dir(),
            "builtin",
            "timestral",
        )
        self.downloads = []
        self.addCleanup(self.restore)
        for model_info in BUILTIN_HF_TRANSFORMERS_MODEL_MAP.values():
            self.addCleanup(setattr, model_info, "state", model_info.state)

    def restore(self):
        os.chdir(self.cwd)
        shutil.rmtree(self.work)

    def fake_download(self, repo_id, filename, local_dir):
        self.downloads.append(repo_id)
        if repo_id != "mistralai/Timestral-209M":
            raise OSError("offline")
        with open(os.path.join(local_dir, filename), "w") as f:
            f.write("{}")
        return os.path.join(local_dir, filename)

    def discover(self):
        with mock.patch(
            "iotdb.ainode.core.model.model_storage.hf_hub_download",
            side_effect=self.fake_download,
        ):
            storage = ModelStorage()
            storage._executor.shutdown(wait=True)
        return storage

    def test_not_downloaded_at_start(self):
        storage = self.discover()
        self.assertNotIn("mistralai/Timestral-209M", self.downloads)
        self.assertIn("thuml/sundial-base-128m", self.downloads)
        self.assertEqual(
            storage.get_model_info("timestral").state, ModelStates.INACTIVE
        )
        self.assertFalse(os.path.exists(os.path.join(self.model_dir, "config.json")))

    def test_downloaded_on_request(self):
        storage = self.discover()
        with mock.patch(
            "iotdb.ainode.core.model.model_storage.hf_hub_download",
            side_effect=self.fake_download,
        ):
            self.assertTrue(storage.download_builtin_model("timestral"))
            # a second request finds the model active and downloads nothing
            count = len(self.downloads)
            self.assertTrue(storage.download_builtin_model("timestral"))
            self.assertEqual(len(self.downloads), count)
        self.assertEqual(storage.get_model_info("timestral").state, ModelStates.ACTIVE)
        self.assertTrue(os.path.exists(os.path.join(self.model_dir, "config.json")))
        self.assertTrue(
            os.path.exists(os.path.join(self.model_dir, "model.safetensors"))
        )

        # once downloaded, the model is active from the next start on
        storage = self.discover()
        self.assertEqual(storage.get_model_info("timestral").state, ModelStates.ACTIVE)

    def test_failed_download_leaves_model_inactive(self):
        storage = self.discover()
        with mock.patch(
            "iotdb.ainode.core.model.model_storage.hf_hub_download",
            side_effect=OSError("offline"),
        ):
            self.assertFalse(storage.download_builtin_model("timestral"))
        self.assertEqual(
            storage.get_model_info("timestral").state, ModelStates.INACTIVE
        )

    def test_forecast_before_download_is_rejected(self):
        storage = self.discover()
        handler = mock.Mock(_model_manager=storage)
        status = AINodeRPCServiceHandler._ensure_model_is_downloaded(
            handler, "timestral"
        )
        self.assertEqual(status.code, TSStatusCode.MODEL_NOT_EXIST_ERROR.value)
        self.assertIn("LOAD MODEL timestral", status.message)
        for model_id in ("sundial", "arima"):
            status = AINodeRPCServiceHandler._ensure_model_is_downloaded(
                handler, model_id
            )
            self.assertEqual(status.code, TSStatusCode.SUCCESS_STATUS.value)


if __name__ == "__main__":
    unittest.main()
