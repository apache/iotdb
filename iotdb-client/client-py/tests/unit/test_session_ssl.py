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
import ssl
from types import SimpleNamespace

import pytest
from thrift.transport import TTransport
from thrift.transport import TSSLSocket


class FakeSslContext:
    def __init__(self):
        self.verify_mode = None
        self.check_hostname = None
        self.verify_locations = []
        self.default_cert_purposes = []
        self.cert_chain = None

    def load_default_certs(self, purpose):
        self.default_cert_purposes.append(purpose)

    def load_verify_locations(self, cafile=None):
        self.verify_locations.append(cafile)

    def load_cert_chain(self, certfile=None, keyfile=None):
        self.cert_chain = (certfile, keyfile)


class FakeSocket:
    def __init__(self, host, port, ssl_context):
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.timeout = None

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


def patch_ssl_transport(monkeypatch, context):
    monkeypatch.setattr(ssl, "create_default_context", lambda purpose: context)
    monkeypatch.setattr(ssl, "SSLContext", lambda protocol: context)
    monkeypatch.setattr(
        TSSLSocket,
        "TSSLSocket",
        lambda host, port, ssl_context: FakeSocket(host, port, ssl_context),
    )
    monkeypatch.setattr(TTransport, "TFramedTransport", FakeTransport)


def test_session_ssl_loads_client_cert_chain(monkeypatch):
    session_module = importlib.import_module("iotdb.Session")
    context = FakeSslContext()
    patch_ssl_transport(monkeypatch, context)

    session = session_module.Session(
        "127.0.0.1",
        "6667",
        use_ssl=True,
        ca_certs="ca.crt",
        client_cert="client.crt",
        client_key="client.key",
    )

    transport = session._Session__get_transport(
        SimpleNamespace(ip="127.0.0.1", port=6667)
    )

    assert context.verify_locations == ["ca.crt"]
    assert context.cert_chain == ("client.crt", "client.key")
    assert transport.socket.ssl_context is context
    assert transport.opened


def test_session_ssl_requires_client_cert_and_key_together(monkeypatch):
    session_module = importlib.import_module("iotdb.Session")
    patch_ssl_transport(monkeypatch, FakeSslContext())

    session = session_module.Session(
        "127.0.0.1", "6667", use_ssl=True, client_cert="client.crt"
    )

    with pytest.raises(TTransport.TTransportException) as exc_info:
        session._Session__get_transport(SimpleNamespace(ip="127.0.0.1", port=6667))

    assert "client_cert and client_key must be set together" in str(exc_info.value)


def test_dbapi_connection_passes_ssl_options(monkeypatch):
    connection_module = importlib.import_module("iotdb.dbapi.Connection")

    class FakeSession:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.open_compression = None

        def open(self, enable_rpc_compression):
            self.open_compression = enable_rpc_compression

        def close(self):
            pass

    monkeypatch.setattr(connection_module, "Session", FakeSession)

    connection = connection_module.Connection(
        "127.0.0.1",
        "6667",
        enable_rpc_compression="true",
        use_ssl="true",
        ca_certs="ca.crt",
        connection_timeout_in_ms="1000",
        client_cert="client.crt",
        client_key="client.key",
    )

    session = connection._Connection__session
    assert session.kwargs["use_ssl"] is True
    assert session.kwargs["ca_certs"] == "ca.crt"
    assert session.kwargs["connection_timeout_in_ms"] == 1000
    assert session.kwargs["client_cert"] == "client.crt"
    assert session.kwargs["client_key"] == "client.key"
    assert session.open_compression is True
