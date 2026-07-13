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

from unittest.mock import Mock

import pytest

from iotdb.ainode.core.manager import cluster_manager
from iotdb.thrift.ainode.ttypes import TAIHeartbeatReq


class FakeAINodeConfig:
    def __init__(self, activated: bool):
        self._activated = activated
        self.set_activated_calls = []

    def is_activated(self) -> bool:
        return self._activated

    def set_activated(self, activated: bool) -> None:
        self.set_activated_calls.append(activated)
        self._activated = activated


def heartbeat_request(activated):
    return TAIHeartbeatReq(
        heartbeatTimestamp=1, needSamplingLoad=False, activated=activated
    )


@pytest.mark.parametrize(
    ("initially_activated", "expected_status"),
    [(False, "UNACTIVATED"), (True, "ACTIVATED")],
)
def test_missing_activation_status_keeps_current_state(
    initially_activated, expected_status, monkeypatch
):
    config = FakeAINodeConfig(initially_activated)
    logger = Mock()
    monkeypatch.setattr(cluster_manager, "AIN_CONFIG", config)
    monkeypatch.setattr(cluster_manager, "logger", logger)

    response = cluster_manager.ClusterManager.get_heart_beat(heartbeat_request(None))

    assert config.is_activated() is initially_activated
    assert config.set_activated_calls == []
    assert response.activateStatus == expected_status
    logger.warning.assert_called_once_with(
        "AINode received a heartbeat without activation status; retaining the current activation status."
    )


@pytest.mark.parametrize(
    ("initially_activated", "requested_status", "expected_status"),
    [(False, True, "ACTIVATED"), (True, False, "UNACTIVATED")],
)
def test_explicit_activation_status_is_applied(
    initially_activated, requested_status, expected_status, monkeypatch
):
    config = FakeAINodeConfig(initially_activated)
    logger = Mock()
    monkeypatch.setattr(cluster_manager, "AIN_CONFIG", config)
    monkeypatch.setattr(cluster_manager, "logger", logger)

    response = cluster_manager.ClusterManager.get_heart_beat(
        heartbeat_request(requested_status)
    )

    assert config.is_activated() is requested_status
    assert config.set_activated_calls == [requested_status]
    assert response.activateStatus == expected_status
    logger.warning.assert_not_called()
