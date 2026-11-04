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

import struct
from unittest.mock import Mock

import pytest
from thrift.transport import TTransport

from iotdb.Session import Session, SessionConnection
from iotdb.thrift.common.ttypes import TSStatus
from iotdb.thrift.rpc.IClientRPCService import Client
from iotdb.thrift.rpc.ttypes import TSExecuteStatementResp, TSFetchResultsResp


def int32_block(timestamp, value):
    # One INT32 value column, one row, plain INT64/INT32 encodings, no nulls.
    return struct.pack(">iBiBB?q?i", 1, 1, 1, 2, 1, False, timestamp, False, value)


@pytest.mark.parametrize(
    "method,args,rpc_method",
    [
        (
            "execute_query_statement",
            ("select s from root.test.d",),
            "executeQueryStatementV2",
        ),
        (
            "execute_statement",
            ("select s from root.test.d",),
            "executeStatementV2",
        ),
        (
            "execute_raw_data_query",
            (["root.test.d.s"], 0, 10),
            "executeRawDataQueryV2",
        ),
        (
            "execute_last_data_query",
            (["root.test.d.s"], 0),
            "executeLastDataQueryV2",
        ),
    ],
)
@pytest.mark.parametrize("reconnect", [False, True])
@pytest.mark.parametrize("early_close", [False, True])
def test_query_fetch_and_close_keep_connection_ids(
    monkeypatch, method, args, rpc_method, reconnect, early_close
):
    # The two generators can advance independently; equal IDs hide a swap.
    initial_ids = (101, 202)
    active_ids = (404, 505) if reconnect else initial_ids
    query_id = 303
    client = Mock(spec=Client)

    def execute(request):
        assert (request.sessionId, request.statementId) == active_ids
        return TSExecuteStatementResp(
            status=TSStatus(code=200),
            queryId=query_id,
            columns=["root.test.d.s"],
            dataTypeList=["INT32"],
            columnNameIndexMap={"root.test.d.s": 0},
            queryResult=[int32_block(1, 42)],
            ignoreTimeStamp=False,
            moreData=True,
        )

    def fetch(request):
        assert (request.sessionId, request.statementId) == active_ids
        assert request.queryId == query_id
        return TSFetchResultsResp(
            status=TSStatus(code=200),
            hasResultSet=True,
            queryResult=[int32_block(2, 43)],
            moreData=False,
        )

    def close(request):
        assert (request.sessionId, request.statementId) == active_ids
        assert request.queryId == query_id
        return TSStatus(code=200)

    getattr(client, rpc_method).side_effect = execute
    client.fetchResultsV2.side_effect = fetch
    client.closeOperation.side_effect = close
    connections = []
    if reconnect:
        failed_client = Mock(spec=Client)

        def fail(request):
            assert (request.sessionId, request.statementId) == initial_ids
            raise TTransport.TTransportException(message="Connection lost")

        getattr(failed_client, rpc_method).side_effect = fail
        connections.append(SessionConnection(failed_client, Mock(), *initial_ids))
    connections.append(SessionConnection(client, Mock(), *active_ids))
    session = Session.init_from_node_urls(["127.0.0.1:6667"])
    init_connection = Mock(side_effect=connections)
    monkeypatch.setattr(session, "init_connection", init_connection)
    session.open()

    with getattr(session, method)(*args) as data_set:
        row = data_set.next()
        assert row.get_timestamp() == 1
        assert row.get_fields()[0].get_int_value() == 42
        if not early_close:
            row = data_set.next()
            assert row.get_timestamp() == 2
            assert row.get_fields()[0].get_int_value() == 43
            assert data_set.next() is None

    assert init_connection.call_count == (2 if reconnect else 1)
    getattr(client, rpc_method).assert_called_once()
    assert client.fetchResultsV2.call_count == (0 if early_close else 1)
    client.closeOperation.assert_called_once()
