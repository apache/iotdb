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

import pytest
from iotdb.Session import Session, _is_wildcard_address


def test_init_from_node_urls_with_bracketed_ipv6():
    session = Session.init_from_node_urls(["[::1]:6667"])

    assert session._Session__hosts == ["::1"]
    assert session._Session__ports == [6667]


def test_init_from_node_urls_with_legacy_ipv6():
    session = Session.init_from_node_urls(["::1:6667"])

    assert session._Session__hosts == ["::1"]
    assert session._Session__ports == [6667]


def test_init_from_node_urls_with_hostname():
    session = Session.init_from_node_urls(["localhost:6667"])

    assert session._Session__hosts == ["localhost"]
    assert session._Session__ports == [6667]


@pytest.mark.parametrize(
    "node_url",
    [
        "[::1]6667",
        "[::1]:",
        "[::1:6667",
    ],
)
def test_init_from_node_urls_rejects_malformed_bracketed_ipv6(node_url):
    with pytest.raises(RuntimeError):
        Session.init_from_node_urls([node_url])


@pytest.mark.parametrize(
    "host",
    [
        "0.0.0.0",
        "[0.0.0.0]",
        "::",
        "[::]",
        "0:0:0:0:0:0:0:0",
        "0::0",
    ],
)
def test_wildcard_address(host):
    assert _is_wildcard_address(host)


@pytest.mark.parametrize(
    "host",
    [
        None,
        "127.0.0.1",
        "192.0.2.1",
        "localhost",
        "example.com",
        "::1",
        "[::1]",
    ],
)
def test_not_wildcard_address(host):
    assert not _is_wildcard_address(host)
