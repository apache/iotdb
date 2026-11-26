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

import importlib
from types import SimpleNamespace

import pytest
from thrift.transport import TTransport

from iotdb.thrift.common.ttypes import TSStatus
from iotdb.utils.exception import StatementExecutionException


class FakeSocket:
    def setTimeout(self, timeout):
        self.timeout = timeout


class FakeTransport:
    def __init__(self, socket):
        self.socket = socket
        self.opened = False

    def isOpen(self):
        return self.opened

    def open(self):
        self.opened = True

    def close(self):
        self.opened = False


@pytest.mark.parametrize(
    "status_code,status_message",
    [
        (801, "Authentication failed."),
        (822, "Account is blocked due to consecutive failed logins."),
    ],
)
def test_session_does_not_retry_iotdb_status_error(
    monkeypatch, status_code, status_message
):
    session_module = importlib.import_module("iotdb.Session")

    class RejectingClient:
        open_session_calls = 0

        def __init__(self, protocol):
            pass

        def openSession(self, request):
            RejectingClient.open_session_calls += 1
            return SimpleNamespace(
                status=TSStatus(code=status_code, message=status_message)
            )

    monkeypatch.setattr(
        session_module.TSocket, "TSocket", lambda host, port: FakeSocket()
    )
    monkeypatch.setattr(session_module.TTransport, "TFramedTransport", FakeTransport)
    monkeypatch.setattr(session_module, "Client", RejectingClient)

    session = session_module.Session.init_from_node_urls(
        ["127.0.0.1:6667"], user="test", password="wrong"
    )

    with pytest.raises(StatementExecutionException) as exc_info:
        session.open()

    assert str(exc_info.value) == f"{status_code}: {status_message}"
    assert RejectingClient.open_session_calls == 1


def test_session_retries_transport_error(monkeypatch):
    session_module = importlib.import_module("iotdb.Session")

    class FailingClient:
        open_session_calls = 0

        def __init__(self, protocol):
            pass

        def openSession(self, request):
            FailingClient.open_session_calls += 1
            raise TTransport.TTransportException(message="Network is unavailable.")

    monkeypatch.setattr(
        session_module.TSocket, "TSocket", lambda host, port: FakeSocket()
    )
    monkeypatch.setattr(session_module.TTransport, "TFramedTransport", FakeTransport)
    monkeypatch.setattr(session_module, "Client", FailingClient)

    session = session_module.Session.init_from_node_urls(
        ["127.0.0.1:6667"], user="test", password="wrong"
    )

    with pytest.raises(session_module.IoTDBConnectionException):
        session.open()

    assert FailingClient.open_session_calls == 1 + session.RETRY_NUM
