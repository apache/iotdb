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
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

rpc_handler_module = types.ModuleType("iotdb.ainode.core.rpc.handler")


class AINodeRPCServiceHandler:
    def __init__(self, ainode):
        self.ainode = ainode


rpc_handler_module.AINodeRPCServiceHandler = AINodeRPCServiceHandler
sys.modules["iotdb.ainode.core.rpc.handler"] = rpc_handler_module

rpc_service_module = types.ModuleType("iotdb.ainode.core.rpc.service")


class AINodeRPCService:
    exit_code = 0

    def __init__(self, handler):
        self.handler = handler

    def start(self):
        pass

    def join(self, timeout=None):
        pass


rpc_service_module.AINodeRPCService = AINodeRPCService
sys.modules["iotdb.ainode.core.rpc.service"] = rpc_service_module

from iotdb.ainode.core import ai_node
from iotdb.ainode.core import config as config_module
from iotdb.thrift.common.ttypes import TEndPoint


class FakeConfig:
    def __init__(self, system_dir: str, ainode_id: int):
        self._system_dir = system_dir
        self._ainode_id = ainode_id

    def get_ain_system_dir(self):
        return self._system_dir

    def get_ainode_id(self):
        return self._ainode_id

    def set_ainode_id(self, ainode_id):
        self._ainode_id = ainode_id

    def get_cluster_name(self):
        return "defaultCluster"

    def get_version_info(self):
        return "test-version"

    def get_build_info(self):
        return "test-build"

    def get_ain_rpc_address(self):
        return "127.0.0.1"

    def get_ain_rpc_port(self):
        return 10810

    def get_ain_target_config_node_list(self):
        return TEndPoint("127.0.0.1", 10710)


class FakeDescriptor:
    def __init__(self, config):
        self._config = config

    def get_config(self):
        return self._config


class AINodeRestartTest(unittest.TestCase):
    def test_descriptor_loads_system_properties_after_configured_system_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            conf_dir = base_dir / "conf"
            default_system_dir = base_dir / "default-system"
            configured_system_dir = base_dir / "configured-system"
            conf_dir.mkdir()
            configured_system_dir.mkdir()
            (conf_dir / "iotdb-ainode.properties").write_text(
                "ain_system_dir={}\n".format(configured_system_dir),
                encoding="utf-8",
            )
            (configured_system_dir / "system.properties").write_text(
                "ainode_id=7\n", encoding="utf-8"
            )

            with mock.patch.object(
                config_module, "AINODE_CONF_DIRECTORY_NAME", str(conf_dir)
            ), mock.patch.object(
                config_module, "AINODE_SYSTEM_DIR", str(default_system_dir)
            ):
                descriptor = config_module.AINodeDescriptor.__wrapped__()

            self.assertEqual(7, descriptor.get_config().get_ainode_id())

    def test_start_with_invalid_local_system_properties_backs_up_and_does_not_connect(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            system_dir = Path(temp_dir)
            system_properties = system_dir / "system.properties"
            system_properties.write_text(
                "cluster_name=defaultCluster\n", encoding="utf-8"
            )
            config = FakeConfig(str(system_dir), -1)

            with mock.patch.object(
                ai_node, "AINodeDescriptor", return_value=FakeDescriptor(config)
            ), mock.patch.object(
                ai_node,
                "ClientManager",
                side_effect=AssertionError("ConfigNode client should not be created"),
            ):
                with self.assertRaises(RuntimeError):
                    ai_node.AINode().start()

            self.assertEqual(-1, config.get_ainode_id())
            self.assertTrue(system_properties.exists())
            self.assertEqual(1, len(list(system_dir.glob("system.properties.*.bak"))))

    def test_restart_failure_backs_up_old_system_properties_and_does_not_register(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            system_dir = Path(temp_dir)
            system_properties = system_dir / "system.properties"
            system_properties.write_text("ainode_id=3\n", encoding="utf-8")
            config = FakeConfig(str(system_dir), 3)
            client = mock.Mock()
            client.node_restart.side_effect = RuntimeError("restart rejected")
            manager = mock.Mock()
            manager.borrow_config_node_client.return_value = client

            with mock.patch.object(
                ai_node, "AINodeDescriptor", return_value=FakeDescriptor(config)
            ), mock.patch.object(
                ai_node, "ClientManager", return_value=manager
            ), mock.patch.object(
                ai_node, "_generate_configuration", return_value=object()
            ), mock.patch.object(
                ai_node, "_generate_version_info", return_value=object()
            ):
                with self.assertRaises(RuntimeError):
                    ai_node.AINode().start()

            self.assertEqual(3, config.get_ainode_id())
            self.assertEqual("ainode_id=3\n", system_properties.read_text("utf-8"))
            self.assertEqual(1, len(list(system_dir.glob("system.properties.*.bak"))))
            client.node_register.assert_not_called()


if __name__ == "__main__":
    unittest.main()
