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

"""Timestral's weights are provided by the user: AINode never downloads them, and the model is
active only once its files are in the local model directory."""

import copy
import json
import os
import shutil
import socket
import tempfile
import unittest
from unittest import mock

import torch
from safetensors.torch import save_file

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.model.model_constants import ModelStates
from iotdb.ainode.core.model.model_info import BUILTIN_HF_TRANSFORMERS_MODEL_MAP
from iotdb.ainode.core.model.model_storage import ModelStorage
from iotdb.ainode.core.model.timestral.modeling_timestral import (
    TimestralForPrediction,
)
from iotdb.ainode.core.model.timestral.pipeline_timestral import TimestralPipeline
from iotdb.ainode.core.rpc.handler import AINodeRPCServiceHandler
from tests.test_timestral import TINY_CONFIG


class TimestralLocalFilesTest(unittest.TestCase):
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
        # No network: any connection, and any Hugging Face download, fails the test
        connect = mock.patch.object(
            socket.socket, "connect", side_effect=AssertionError("network access")
        )
        connect.start()
        self.addCleanup(connect.stop)

    def restore(self):
        os.chdir(self.cwd)
        shutil.rmtree(self.work)

    def fake_download(self, repo_id, filename, local_dir):
        # The other built-in models still download at start, as before
        self.downloads.append(repo_id)
        raise OSError("offline")

    def discover(self):
        with mock.patch(
            "iotdb.ainode.core.model.model_storage.hf_hub_download",
            side_effect=self.fake_download,
        ):
            storage = ModelStorage()
            storage._executor.shutdown(wait=True)
        return storage

    def install(self, files=("config.json", "model.safetensors", "LICENSE")):
        """Place the files of a small Timestral checkpoint, as a user would."""
        os.makedirs(self.model_dir, exist_ok=True)
        torch.manual_seed(0)
        model = TimestralForPrediction(**copy.deepcopy(TINY_CONFIG)).eval()
        if "config.json" in files:
            with open(os.path.join(self.model_dir, "config.json"), "w") as f:
                json.dump(TINY_CONFIG, f)
        if "model.safetensors" in files:
            save_file(
                model.state_dict(), os.path.join(self.model_dir, "model.safetensors")
            )
        if "LICENSE" in files:
            with open(os.path.join(self.model_dir, "LICENSE"), "w") as f:
                f.write("The licence of the weights.\n")

    def test_never_downloaded_and_inactive_without_its_files(self):
        storage = self.discover()
        self.assertEqual(
            storage.get_model_info("timestral").state, ModelStates.INACTIVE
        )
        # the other built-in models went through their download at start, Timestral did not
        self.assertIn("thuml/sundial-base-128m", self.downloads)
        self.assertEqual(
            len(self.downloads), len(BUILTIN_HF_TRANSFORMERS_MODEL_MAP) - 1
        )
        self.assertTrue(os.path.isdir(self.model_dir))  # created for the user to fill

    def test_missing_files_give_setup_instructions(self):
        storage = self.discover()
        self.install(files=("config.json",))
        with mock.patch(
            "iotdb.ainode.core.model.model_storage.hf_hub_download",
            side_effect=AssertionError("download"),
        ):
            message = storage.validate_user_provided_model("timestral")
        self.assertIn("model.safetensors, LICENSE not found", message)
        self.assertIn(os.path.abspath(self.model_dir), message)
        self.assertIn("https://huggingface.co/mistralai/Timestral-209M", message)
        self.assertIn("USD 20 million", message)
        self.assertEqual(
            storage.get_model_info("timestral").state, ModelStates.INACTIVE
        )

    def test_unreadable_files_are_rejected(self):
        storage = self.discover()
        self.install()
        with open(os.path.join(self.model_dir, "config.json"), "w") as f:
            f.write("{not json")
        self.assertIn(
            "config.json cannot be read",
            storage.validate_user_provided_model("timestral"),
        )
        self.install()
        with open(os.path.join(self.model_dir, "model.safetensors"), "wb") as f:
            f.write(b"not a safetensors file")
        self.assertIn(
            "model.safetensors is not a safetensors file",
            storage.validate_user_provided_model("timestral"),
        )
        self.assertEqual(
            storage.get_model_info("timestral").state, ModelStates.INACTIVE
        )

    def test_manually_installed_files_load_and_forecast(self):
        storage = self.discover()
        # files placed after AINode started are found when the model is loaded
        self.install()
        self.assertIsNone(storage.validate_user_provided_model("timestral"))
        model_info = storage.get_model_info("timestral")
        self.assertEqual(model_info.state, ModelStates.ACTIVE)
        pipeline = TimestralPipeline(model_info, device=torch.device("cpu"))
        kwargs = {"output_length": 8}
        series = pipeline.preprocess([{"targets": torch.randn(40).cumsum(0)}], **kwargs)
        output = pipeline.postprocess(pipeline.forecast(series, **kwargs), **kwargs)
        self.assertEqual(tuple(output[0].shape), (1, 8))
        # and the model is active from the next start on
        self.assertEqual(
            self.discover().get_model_info("timestral").state, ModelStates.ACTIVE
        )

    def test_loader_never_falls_back_to_the_hub(self):
        with self.assertRaises(FileNotFoundError):
            TimestralForPrediction.from_pretrained(os.path.join(self.work, "missing"))

    def test_load_model_and_forecast_without_files_are_rejected(self):
        storage = self.discover()
        handler = mock.Mock(_model_manager=storage)
        check = AINodeRPCServiceHandler._ensure_user_provided_model_is_installed
        status = check(
            handler, "timestral", TSStatusCode.LOAD_MODEL_ERROR.value, recheck=True
        )
        self.assertEqual(status.code, TSStatusCode.LOAD_MODEL_ERROR.value)
        self.assertIn("is not installed", status.message)
        status = check(
            handler,
            "timestral",
            TSStatusCode.MODEL_NOT_EXIST_ERROR.value,
            recheck=False,
        )
        self.assertEqual(status.code, TSStatusCode.MODEL_NOT_EXIST_ERROR.value)
        # once installed, LOAD MODEL and forecasts pass
        self.install()
        for recheck in (True, False):
            status = check(
                handler, "timestral", TSStatusCode.LOAD_MODEL_ERROR.value, recheck
            )
            self.assertEqual(status.code, TSStatusCode.SUCCESS_STATUS.value)

    def test_other_models_are_not_checked(self):
        storage = self.discover()
        handler = mock.Mock(_model_manager=mock.Mock(wraps=storage))
        check = AINodeRPCServiceHandler._ensure_user_provided_model_is_installed
        for model_id in ("sundial", "chronos2", "arima"):
            for recheck in (True, False):
                status = check(
                    handler, model_id, TSStatusCode.LOAD_MODEL_ERROR.value, recheck
                )
                self.assertEqual(status.code, TSStatusCode.SUCCESS_STATUS.value)
        handler._model_manager.validate_user_provided_model.assert_not_called()


if __name__ == "__main__":
    unittest.main()
