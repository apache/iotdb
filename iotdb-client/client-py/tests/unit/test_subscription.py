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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import struct
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from thrift.Thrift import TMessageType
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

from iotdb.Session import Session
from iotdb.subscription import (
    Subscription,
    SubscriptionError,
    SubscriptionSession,
    SubscriptionPullConsumer,
    _CommitContext,
    _decode_heartbeat_endpoints,
    _decode_poll_response,
    _poll_body,
    _poll_file_body,
    _read_commit_context,
    _read_string_map,
    _safe_path_component,
    _write_commit_context,
    _write_string,
    _write_string_map,
)
from iotdb.thrift.rpc import IClientRPCService
from iotdb.thrift.rpc.ttypes import TPipeSubscribeReq


class _Response(object):
    def __init__(self, body=None):
        self.status = type("Status", (), {"code": 200, "subStatus": []})()
        self.body = [body] if body is not None else []


def _consumer_config():
    return {
        "node_urls": ["127.0.0.1:6667"],
        "username": "root",
        "password": "root",
        "sql_dialect": "table",
        "database": None,
        "time_zone": "UTC",
        "enable_redirection": True,
        "enable_compression": False,
        "use_ssl": False,
        "ca_certs": None,
        "connection_timeout_in_ms": None,
        "client_cert": None,
        "client_key": None,
    }


def _heartbeat_body(endpoints, topics=(), unsubscribed_topics=()):
    body = [struct.pack(">i", len(topics))]
    for topic_name, attributes in topics:
        body.extend([_write_string(topic_name), _write_string_map(attributes)])
    body.append(struct.pack(">i", len(endpoints)))
    for node_id, host, port in endpoints:
        body.extend(
            [struct.pack(">i", node_id), _write_string(host), struct.pack(">i", port)]
        )
    body.append(struct.pack(">i", len(unsubscribed_topics)))
    body.extend(_write_string(topic_name) for topic_name in unsubscribed_topics)
    return b"".join(body)


def _file_piece(file_name, offset, contents, context):
    next_offset = offset + len(contents)
    return b"".join(
        [
            struct.pack(">h", 3),
            _write_string(file_name),
            struct.pack(">q", next_offset),
            struct.pack(">i", len(contents)),
            contents,
            _write_commit_context(context),
            b"\x01",
        ]
    )


def _file_seal(file_name, file_length, database_name, context):
    return b"".join(
        [
            struct.pack(">h", 4),
            _write_string(file_name),
            struct.pack(">q", file_length),
            _write_string(database_name),
            _write_commit_context(context),
            b"\x01",
        ]
    )


def _watermark(timestamp, data_node_id, context):
    return b"".join(
        [
            struct.pack(">h", 7),
            struct.pack(">q", timestamp),
            struct.pack(">i", data_node_id),
            _write_commit_context(context),
            b"\x01",
        ]
    )


class SubscriptionProtocolTest(unittest.TestCase):
    def test_pipe_subscribe_keeps_request_with_accelerated_protocol(self):
        transport = TTransport.TMemoryBuffer()
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
        client = IClientRPCService.Client(protocol)
        response = _Response()
        client.recv_pipeSubscribe = mock.Mock(return_value=response)
        session = Session.__new__(Session)
        session._Session__is_close = False
        session._Session__client = client
        request = TPipeSubscribeReq(version=1, type=0, body=b"handshake")
        fast_encode = protocol._fast_encode

        self.assertIs(response, session._pipe_subscribe(request))
        self.assertIs(fast_encode, protocol._fast_encode)

        reader = TBinaryProtocol.TBinaryProtocol(
            TTransport.TMemoryBuffer(transport.getvalue())
        )
        method, message_type, sequence_id = reader.readMessageBegin()
        arguments = IClientRPCService.pipeSubscribe_args()
        arguments.read(reader)
        reader.readMessageEnd()
        self.assertEqual("pipeSubscribe", method)
        self.assertEqual(TMessageType.CALL, message_type)
        self.assertEqual(request, arguments.req)

    def test_poll_request_matches_subscription_wire_layout(self):
        expected = b"".join(
            [
                struct.pack(">h", 0),
                struct.pack(">i", 1),
                b"\x06",
                _write_string("topicA"),
                struct.pack(">qqi", 1234, 4096, 0),
            ]
        )
        self.assertEqual(expected, _poll_body({"topicA"}, 1234, 4096))

    def test_commit_context_round_trip_including_writer_progress(self):
        original = _CommitContext(
            1,
            2,
            "topic",
            "group",
            3,
            0,
            "region",
            9,
            writer_id=("region", 7),
            writer_progress=(9, 10),
        )
        parsed = _read_commit_context(io.BytesIO(_write_commit_context(original)))
        self.assertEqual(original, parsed)

    def test_poll_file_request_includes_context_offset_timeout_and_limit(self):
        context = _CommitContext(1, 2, "topic", "group", 3, 0, "", 0)
        expected = b"".join(
            [
                struct.pack(">h", 1),
                _write_commit_context(context),
                struct.pack(">qqqi", 17, 500, 4096, 0),
            ]
        )
        self.assertEqual(expected, _poll_file_body(context, 17, 500, 4096))

    def test_decodes_sealed_tsfile_response(self):
        context = _CommitContext(1, 2, "topic", "group", 3, 0, "", 0)
        body = b"".join(
            [
                struct.pack(">h", 4),
                _write_string("data.tsfile"),
                struct.pack(">q", 12),
                _write_string("root.sg"),
                _write_commit_context(context),
                b"\x01",
                struct.pack(">i", 0),
            ]
        )
        parsed = _decode_poll_response(body)
        self.assertEqual("data.tsfile", parsed["file_name"])
        self.assertEqual(12, parsed["file_length"])
        self.assertEqual("root.sg", parsed["database_name"])
        self.assertEqual(context, parsed["context"])
        self.assertTrue(parsed["time_selected"])

    def test_decodes_watermark_response(self):
        context = _CommitContext(7, 2, "snapshot", "export", 9, 0, "", 0)
        parsed = _decode_poll_response(_watermark(123456, 7, context))

        self.assertEqual(7, parsed["type"])
        self.assertEqual(123456, parsed["watermark_timestamp"])
        self.assertEqual(7, parsed["data_node_id"])
        self.assertEqual(context, parsed["context"])
        self.assertTrue(parsed["time_selected"])

    def test_poll_consumes_watermarks_and_tracks_greatest_timestamp(self):
        context = _CommitContext(7, 0, "snapshot", "export", 9, 0, "", 0)
        responses = [
            _watermark(123456, 7, context),
            _watermark(120000, 7, context),
        ]
        consumer = SubscriptionPullConsumer(_consumer_config())
        consumer._open = True
        consumer._topics.add("snapshot")
        consumer._providers[7] = ("127.0.0.1", 6667, object())
        consumer._send = lambda _session, _request_type, _body=None: _Response(
            responses.pop(0)
        )

        self.assertEqual([], consumer.poll(timeout_ms=1000))
        self.assertEqual(123456, consumer.get_latest_watermark_timestamp())
        self.assertEqual([], consumer.poll(timeout_ms=1000))
        self.assertEqual(123456, consumer.latest_watermark_timestamp)
        consumer.close()

    def test_decodes_endpoint_map_from_heartbeat(self):
        body = b"".join(
            [
                struct.pack(">i", 1),
                _write_string("topicA"),
                _write_string_map({"mode": "snapshot"}),
                struct.pack(">i", 1),
                struct.pack(">i", 7),
                _write_string("127.0.0.1"),
                struct.pack(">i", 6667),
                struct.pack(">i", 1),
                _write_string("topic_done"),
            ]
        )
        self.assertEqual(
            ({7: ("127.0.0.1", 6667)}, ["topic_done"]),
            _decode_heartbeat_endpoints(_Response(body)),
        )

    def test_handshake_and_provider_sync_preserve_identity_and_subscriptions(self):
        class _ProviderSession(object):
            def __init__(self, node_id):
                self.node_id = node_id
                self.closed = False

            def _get_server_thrift_max_frame_size(self, fallback):
                return fallback

            def close(self):
                self.closed = True

        sessions = {}
        handshake_configurations = {}
        requests = []
        advertised_endpoints = [
            (7, "127.0.0.1", 6667),
            (8, "127.0.0.1", 6668),
        ]

        def open_provider_session(config, host, port):
            node_id = port - 6660
            session = _ProviderSession(node_id)
            sessions[node_id] = session
            return session

        consumer = SubscriptionPullConsumer(
            _consumer_config(),
            consumer_group_id="dataset-import",
            consumer_id="rest-worker",
            heartbeat_interval_ms=300000,
        )

        def send(session, request_type, body=None):
            requests.append((session.node_id, request_type, body))
            if request_type == 0:
                handshake_configurations[session.node_id] = _read_string_map(
                    io.BytesIO(body)
                )
                identity = struct.pack(">i", session.node_id)
                identity += _write_string("rest-worker")
                identity += _write_string("dataset-import")
                return _Response(identity)
            if request_type == 1:
                return _Response(_heartbeat_body(advertised_endpoints))
            return _Response()

        with mock.patch(
            "iotdb.subscription._open_provider_session",
            side_effect=open_provider_session,
        ):
            consumer._send = send
            consumer.open()
            try:
                self.assertEqual({7, 8}, set(consumer._providers))
                self.assertEqual("table", handshake_configurations[7]["sql-dialect"])
                self.assertEqual(
                    "rest-worker", handshake_configurations[7]["consumer-id"]
                )
                self.assertEqual(
                    "dataset-import", handshake_configurations[7]["group-id"]
                )
                self.assertTrue(handshake_configurations[7]["consumer-instance-id"])

                consumer.subscribe("snapshot_topic")
                advertised_endpoints = [
                    (7, "127.0.0.1", 6667),
                    (8, "127.0.0.1", 6668),
                    (9, "127.0.0.1", 6669),
                ]
                consumer._sync_providers()

                self.assertEqual({7, 8, 9}, set(consumer._providers))
                self.assertIn(9, handshake_configurations)
                self.assertTrue(
                    any(
                        node_id == 9 and request_type == 5
                        for node_id, request_type, _ in requests
                    ),
                    "a newly discovered DataNode must subscribe to current topics",
                )

                advertised_endpoints = [
                    (7, "127.0.0.1", 6667),
                    (9, "127.0.0.1", 6669),
                ]
                consumer._sync_providers()

                self.assertEqual({7, 9}, set(consumer._providers))
                self.assertTrue(sessions[8].closed)
            finally:
                consumer.close()

    def test_topic_sql_escapes_attribute_literals(self):
        session = object.__new__(SubscriptionSession)
        statements = []

        class _SqlSession(object):
            def execute_non_query_statement(self, sql):
                statements.append(sql)

        session._session = _SqlSession()
        session.create_topic("snapshot", {"database": "root.o'k"})
        self.assertEqual(
            "CREATE TOPIC `snapshot` WITH ('database'='root.o''k')", statements[0]
        )

    def test_topic_subscription_management_uses_iotdb_sql(self):
        class _Field(object):
            def __init__(self, value):
                self.value = value

            def get_string_value(self):
                return self.value

        class _Record(object):
            def __init__(self, values):
                self.fields = [_Field(value) for value in values]

            def get_fields(self):
                return self.fields

        class _DataSet(object):
            def __init__(self, rows):
                self.rows = iter(rows)
                self.closed = False

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.closed = True

            def has_next(self):
                if not hasattr(self, "next_row"):
                    try:
                        self.next_row = next(self.rows)
                    except StopIteration:
                        return False
                return True

            def next(self):
                record = self.next_row
                del self.next_row
                return record

        data_set = _DataSet(
            [
                _Record(
                    [
                        "snapshot_group_123",
                        "snapshot`topic",
                        "group",
                        "[consumer]",
                    ]
                )
            ]
        )
        statements = []

        class _SqlSession(object):
            def execute_query_statement(self, sql):
                statements.append(sql)
                return data_set

            def execute_non_query_statement(self, sql):
                statements.append(sql)

        session = object.__new__(SubscriptionSession)
        session._session = _SqlSession()

        subscriptions = session.get_subscriptions("snapshot`topic")
        session.drop_subscription_if_exists("snapshot_group_123`old")

        self.assertEqual(
            [
                "SHOW SUBSCRIPTIONS ON `snapshot``topic`",
                "DROP SUBSCRIPTION IF EXISTS `snapshot_group_123``old`",
            ],
            statements,
        )
        self.assertTrue(data_set.closed)
        self.assertEqual(1, len(subscriptions))
        self.assertIsInstance(subscriptions[0], Subscription)
        self.assertEqual("snapshot_group_123", subscriptions[0].subscription_id)
        self.assertEqual("snapshot`topic", subscriptions[0].topic_name)
        self.assertEqual("group", subscriptions[0].consumer_group_id)
        self.assertEqual("[consumer]", subscriptions[0].consumer_ids)

    def test_path_components_are_safe_for_local_paths(self):
        self.assertEqual("%2E%2E", _safe_path_component(".."))
        self.assertEqual("root%2Fsg", _safe_path_component("root/sg"))
        with self.assertRaises(SubscriptionError):
            _safe_path_component("")

    def test_consumer_receives_and_commits_snapshot_file(self):
        config = {
            "node_urls": ["127.0.0.1:6667"],
            "username": "root",
            "password": "root",
            "sql_dialect": "table",
            "database": None,
            "time_zone": "UTC",
            "enable_redirection": True,
            "enable_compression": False,
            "use_ssl": False,
            "ca_certs": None,
            "connection_timeout_in_ms": None,
            "client_cert": None,
            "client_key": None,
        }
        context = _CommitContext(7, 0, "snapshot", "export", 9, 0, "", 0)
        contents = b"tsfile-content"
        calls = []

        def send(session, request_type, body=None):
            calls.append(request_type)
            if request_type == 0:
                identity = struct.pack(">i", 7) + _write_string("consumer")
                identity += _write_string("export")
                return _Response(identity)
            if request_type == 1:
                heartbeat = b"".join(
                    [
                        struct.pack(">i", 0),
                        struct.pack(">i", 1),
                        struct.pack(">i", 7),
                        _write_string("127.0.0.1"),
                        struct.pack(">i", 6667),
                        struct.pack(">i", 0),
                    ]
                )
                return _Response(heartbeat)
            if request_type == 2:
                poll_type = struct.unpack(">h", body[:2])[0]
                if poll_type == 0:
                    if calls.count(2) >= 4:
                        response_body = struct.pack(">h", 5)
                        response_body += _write_commit_context(context) + b"\x01"
                        response_body += struct.pack(">i", 0)
                    else:
                        response_body = struct.pack(">h", 2) + _write_string(
                            "snapshot.tsfile"
                        )
                        response_body += _write_commit_context(context) + b"\x01"
                        response_body += struct.pack(">i", 0)
                else:
                    piece = struct.pack(">h", 3) + _write_string("snapshot.tsfile")
                    piece += struct.pack(">q", len(contents))
                    piece += struct.pack(">i", len(contents)) + contents
                    piece += _write_commit_context(context) + b"\x01"
                    piece += struct.pack(">i", 0)
                    sealed = struct.pack(">h", 4) + _write_string("snapshot.tsfile")
                    sealed += struct.pack(">q", len(contents)) + _write_string(
                        "root.sg"
                    )
                    sealed += _write_commit_context(context) + b"\x01"
                    sealed += struct.pack(">i", 0)
                    response_body = piece if calls.count(2) == 2 else sealed
                return _Response(response_body)
            if request_type == 3:
                accepted = struct.pack(">i", 1) + _write_commit_context(context)
                accepted += struct.pack(">i", 0)
                return _Response(accepted)
            return _Response()

        with tempfile.TemporaryDirectory() as temp_dir:
            consumer = SubscriptionPullConsumer(
                config,
                consumer_group_id="export",
                consumer_id="consumer",
                file_save_dir=temp_dir,
            )
            with mock.patch(
                "iotdb.subscription._open_provider_session",
                return_value=type(
                    "ProviderSession",
                    (),
                    {
                        "_get_server_thrift_max_frame_size": lambda self, fallback: fallback,
                        "close": lambda self: None,
                    },
                )(),
            ):
                consumer._send = send
                consumer.open()
                try:
                    consumer.subscribe("snapshot")
                    messages = consumer.poll(timeout_ms=1000)
                    self.assertEqual(1, len(messages))
                    with open(messages[0].file_path, "rb") as tsfile:
                        self.assertEqual(contents, tsfile.read())
                    self.assertEqual("root.sg", messages[0].database_name)
                    consumer.commit(messages[0])
                    self.assertIn(3, calls)
                    self.assertEqual([], consumer.poll(timeout_ms=1000))
                    self.assertTrue(consumer.all_topic_messages_have_been_consumed())
                    self.assertIn(6, calls)
                finally:
                    consumer.close()

    def test_file_transfer_resumes_at_offset_and_verifies_sealed_length(self):
        context = _CommitContext(7, 0, "snapshot", "export", 9, 0, "", 0)
        request_offsets = []
        responses = [
            _file_piece("snapshot.tsfile", 2, b"cd", context),
            _file_piece("snapshot.tsfile", 4, b"e", context),
            _file_seal("snapshot.tsfile", 5, "root.sg", context),
        ]
        consumer = SubscriptionPullConsumer(_consumer_config())
        consumer._open = True
        provider_session = object()
        consumer._providers[7] = ("127.0.0.1", 6667, provider_session)

        def send(session, request_type, body=None):
            self.assertEqual(2, request_type)
            self.assertEqual(1, struct.unpack(">h", body[:2])[0])
            reader = io.BytesIO(body[2:])
            self.assertEqual(context, _read_commit_context(reader))
            request_offsets.append(struct.unpack(">q", reader.read(8))[0])
            return _Response(responses.pop(0))

        consumer._send = send
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = (
                Path(temp_dir) / "export" / "consumer" / "snapshot" / "snapshot.tsfile"
            )
            file_path.parent.mkdir(parents=True)
            file_path.write_bytes(b"ab")
            transfer = {
                "node_id": 7,
                "context": context,
                "file_name": "snapshot.tsfile",
                "path": file_path,
                "offset": 2,
            }

            message = consumer._continue_file_transfer(transfer, time.monotonic() + 2)

            self.assertIsNotNone(message)
            self.assertEqual([2, 4, 5], request_offsets)
            self.assertEqual(b"abcde", file_path.read_bytes())
            self.assertEqual("root.sg", message.database_name)
        consumer.close()

    def test_file_transfer_reconnects_and_resumes_existing_offset_through_seal(self):
        context = _CommitContext(7, 0, "snapshot", "export", 9, 0, "", 0)
        sessions = []
        poll_offsets = []
        subscribed_sessions = []

        class _ProviderSession(object):
            def __init__(self, generation):
                self.generation = generation

            def _get_server_thrift_max_frame_size(self, fallback):
                return fallback

            def close(self):
                pass

        def open_provider_session(_config, _host, _port):
            session = _ProviderSession(len(sessions) + 1)
            sessions.append(session)
            return session

        def pipe_subscribe(session, request):
            if request.type == 0:
                identity = struct.pack(">i", 7) + _write_string("consumer")
                identity += _write_string("export")
                return _Response(identity)
            if request.type == 5:
                subscribed_sessions.append(session.generation)
                return _Response()
            if request.type == 2:
                self.assertEqual(1, struct.unpack(">h", request.body[:2])[0])
                reader = io.BytesIO(request.body[2:])
                self.assertEqual(context, _read_commit_context(reader))
                offset = struct.unpack(">q", reader.read(8))[0]
                poll_offsets.append((session.generation, offset))
                if session.generation == 0:
                    raise TTransport.TTransportException(
                        type=TTransport.TTransportException.END_OF_FILE
                    )
                if offset == 3:
                    return _Response(_file_piece("snapshot.tsfile", 3, b"def", context))
                return _Response(_file_seal("snapshot.tsfile", 6, "root.sg", context))
            return _Response()

        consumer = SubscriptionPullConsumer(
            _consumer_config(),
            consumer_group_id="export",
            consumer_id="consumer",
            heartbeat_interval_ms=300000,
        )
        consumer._open = True
        consumer._topics.add("snapshot")
        old_session = _ProviderSession(0)
        consumer._providers[7] = ("127.0.0.1", 6667, old_session)
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "export" / "consumer" / "snapshot.tsfile"
            file_path.parent.mkdir(parents=True)
            file_path.write_bytes(b"abc")
            transfer = {
                "node_id": 7,
                "context": context,
                "file_name": "snapshot.tsfile",
                "path": file_path,
                "offset": 3,
            }

            with mock.patch(
                "iotdb.subscription._open_provider_session",
                side_effect=open_provider_session,
            ), mock.patch(
                "iotdb.subscription._pipe_subscribe",
                side_effect=pipe_subscribe,
            ):
                message = consumer._continue_file_transfer(
                    transfer, time.monotonic() + 2
                )

            self.assertIsNotNone(message)
            self.assertEqual(b"abcdef", file_path.read_bytes())
            self.assertEqual("root.sg", message.database_name)

        self.assertEqual([(0, 3), (1, 3), (1, 6)], poll_offsets)
        self.assertEqual([1], subscribed_sessions)
        self.assertEqual(1, len(sessions))
        consumer.close()

    def test_file_transfer_rejects_piece_with_inconsistent_next_offset(self):
        context = _CommitContext(7, 0, "snapshot", "export", 9, 0, "", 0)
        malformed_piece = b"".join(
            [
                struct.pack(">h", 3),
                _write_string("snapshot.tsfile"),
                struct.pack(">q", 4),
                struct.pack(">i", 2),
                b"ab",
                _write_commit_context(context),
                b"\x01",
            ]
        )
        consumer = SubscriptionPullConsumer(_consumer_config())
        consumer._open = True
        consumer._providers[7] = ("127.0.0.1", 6667, object())
        consumer._send = lambda session, request_type, body=None: _Response(
            malformed_piece
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "snapshot.tsfile"
            transfer = {
                "node_id": 7,
                "context": context,
                "file_name": "snapshot.tsfile",
                "path": file_path,
                "offset": 0,
            }
            with self.assertRaisesRegex(SubscriptionError, "offset is inconsistent"):
                consumer._continue_file_transfer(transfer, time.monotonic() + 1)
        consumer.close()

    def test_file_transfer_rejects_seal_when_received_length_differs(self):
        context = _CommitContext(7, 0, "snapshot", "export", 9, 0, "", 0)
        consumer = SubscriptionPullConsumer(_consumer_config())
        consumer._open = True
        consumer._providers[7] = ("127.0.0.1", 6667, object())
        consumer._send = lambda session, request_type, body=None: _Response(
            _file_seal("snapshot.tsfile", 4, "root.sg", context)
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "snapshot.tsfile"
            file_path.write_bytes(b"abc")
            transfer = {
                "node_id": 7,
                "context": context,
                "file_name": "snapshot.tsfile",
                "path": file_path,
                "offset": 3,
            }
            with self.assertRaisesRegex(SubscriptionError, "does not match its seal"):
                consumer._continue_file_transfer(transfer, time.monotonic() + 1)
        consumer.close()

    def test_file_transfer_rejects_oversized_piece_before_writing(self):
        context = _CommitContext(7, 0, "snapshot", "export", 9, 0, "", 0)
        consumer = SubscriptionPullConsumer(_consumer_config(), max_file_bytes=5)
        consumer._open = True
        consumer._providers[7] = ("127.0.0.1", 6667, object())
        consumer._send = lambda _session, _request_type, _body=None: _Response(
            _file_piece("snapshot.tsfile", 4, b"ef", context)
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "snapshot.tsfile"
            file_path.write_bytes(b"abcd")
            transfer = {
                "node_id": 7,
                "context": context,
                "file_name": "snapshot.tsfile",
                "path": file_path,
                "offset": 4,
            }
            with self.assertRaisesRegex(SubscriptionError, "size limit"):
                consumer._continue_file_transfer(transfer, time.monotonic() + 1)
            self.assertEqual(b"abcd", file_path.read_bytes())
        consumer.close()

    def test_file_transfer_rejects_oversized_existing_partial_file(self):
        context = _CommitContext(7, 0, "snapshot", "export", 9, 0, "", 0)
        consumer = SubscriptionPullConsumer(_consumer_config(), max_file_bytes=5)
        consumer._open = True
        consumer._providers[7] = ("127.0.0.1", 6667, object())
        poll_attempts = []

        def send(_session, request_type, body=None):
            if request_type == 2:
                poll_attempts.append(request_type)
            return _Response()

        consumer._send = send

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "snapshot.tsfile"
            file_path.write_bytes(b"abcdef")
            transfer = {
                "node_id": 7,
                "context": context,
                "file_name": "snapshot.tsfile",
                "path": file_path,
                "offset": 6,
            }
            with self.assertRaisesRegex(SubscriptionError, "size limit"):
                consumer._continue_file_transfer(transfer, time.monotonic() + 1)
            self.assertEqual(b"abcdef", file_path.read_bytes())
            self.assertEqual([], poll_attempts)
        consumer.close()

    def test_commit_rejects_acknowledgement_for_a_different_context(self):
        context = _CommitContext(7, 1, "snapshot", "export", 9, 0, "", 0)
        different_context = _CommitContext(7, 1, "snapshot", "export", 10, 0, "", 0)
        consumer = SubscriptionPullConsumer(_consumer_config())
        consumer._open = True
        consumer._providers[7] = ("127.0.0.1", 6667, object())
        consumer._send = lambda session, request_type, body=None: _Response(
            struct.pack(">i", 1) + _write_commit_context(different_context)
        )
        message = type("Message", (), {"_commit_context": context})()

        with self.assertRaisesRegex(SubscriptionError, "did not accept"):
            consumer.commit(message)
        consumer.close()

    def test_poll_surfaces_disconnection_instead_of_reporting_no_messages(self):
        consumer = SubscriptionPullConsumer(_consumer_config())
        consumer._open = True
        consumer._topics.add("snapshot_topic")
        consumer._providers[7] = ("127.0.0.1", 6667, object())

        def disconnected(session, request_type, body=None):
            raise OSError("connection reset")

        consumer._send = disconnected
        with self.assertRaisesRegex(OSError, "connection reset"):
            consumer.poll(timeout_ms=100)
        self.assertEqual({"snapshot_topic"}, consumer._topics)
        consumer.close()

    def test_poll_reconnects_data_node_and_restores_subscription(self):
        sessions = []
        poll_attempts = []
        subscriptions = []

        class _ProviderSession(object):
            def __init__(self, generation):
                self.generation = generation

            def _get_server_thrift_max_frame_size(self, fallback):
                return fallback

            def close(self):
                pass

        def open_provider_session(_config, _host, _port):
            session = _ProviderSession(len(sessions))
            sessions.append(session)
            return session

        def response(body=None):
            return _Response(body)

        def pipe_subscribe(session, request):
            if request.type == 0:
                identity = struct.pack(">i", 7) + _write_string("consumer")
                identity += _write_string("group")
                return response(identity)
            if request.type == 1:
                return response(_heartbeat_body([(7, "127.0.0.1", 6667)]))
            if request.type == 5:
                subscriptions.append(session.generation)
                return response()
            if request.type == 2:
                poll_attempts.append(session.generation)
                if session.generation == 0:
                    raise TTransport.TTransportException(
                        type=TTransport.TTransportException.END_OF_FILE
                    )
                return response()
            return response()

        consumer = SubscriptionPullConsumer(
            _consumer_config(),
            consumer_group_id="group",
            consumer_id="consumer",
            heartbeat_interval_ms=300000,
        )
        with mock.patch(
            "iotdb.subscription._open_provider_session",
            side_effect=open_provider_session,
        ), mock.patch(
            "iotdb.subscription._pipe_subscribe",
            side_effect=pipe_subscribe,
        ):
            consumer.open()
            try:
                consumer.subscribe("snapshot")
                self.assertEqual([], consumer.poll(timeout_ms=100))
            finally:
                consumer.close()

        self.assertEqual([0, 1], poll_attempts)
        self.assertEqual([0, 1], subscriptions)
        self.assertEqual(2, len(sessions))

    def test_poll_decoder_rejects_malformed_frames(self):
        truncated_piece = b"".join(
            [
                struct.pack(">h", 3),
                _write_string("snapshot.tsfile"),
                struct.pack(">q", 4),
                struct.pack(">i", 3),
                b"ab",
            ]
        )
        malformed_frames = [
            b"\x00",
            struct.pack(">h", 99),
            truncated_piece,
        ]
        for frame in malformed_frames:
            with self.subTest(frame=frame):
                with self.assertRaises(SubscriptionError):
                    _decode_poll_response(frame)


if __name__ == "__main__":
    unittest.main()
