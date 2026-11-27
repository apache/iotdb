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
import os
import struct
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote

from iotdb.Session import Session
from iotdb.thrift.rpc.ttypes import TPipeSubscribeReq
from iotdb.utils import rpc_utils
from iotdb.utils.exception import IoTDBConnectionException
from thrift.transport import TTransport


class SubscriptionError(Exception):
    """Raised when an IoTDB Subscription RPC returns an error."""


class SubscriptionMessage(object):
    """A completed Subscription TsFile and the context required to acknowledge it."""

    def __init__(self, file_path, topic_name, database_name, commit_context):
        self.file_path = str(file_path)
        self.topic_name = topic_name
        self.database_name = database_name
        self._commit_context = commit_context


class Subscription(object):
    """A remote Subscription registered for a topic and consumer group."""

    def __init__(self, subscription_id, topic_name, consumer_group_id, consumer_ids):
        self.subscription_id = subscription_id
        self.topic_name = topic_name
        self.consumer_group_id = consumer_group_id
        self.consumer_ids = consumer_ids


class _CommitContext(object):
    def __init__(
        self,
        data_node_id,
        reboot_times,
        topic_name,
        consumer_group_id,
        commit_id,
        seek_generation,
        region_id,
        physical_time,
        writer_id=None,
        writer_progress=None,
    ):
        self.data_node_id = data_node_id
        self.reboot_times = reboot_times
        self.topic_name = topic_name
        self.consumer_group_id = consumer_group_id
        self.commit_id = commit_id
        self.seek_generation = seek_generation
        self.region_id = region_id
        self.physical_time = physical_time
        self.writer_id = writer_id
        self.writer_progress = writer_progress

    def __eq__(self, other):
        return isinstance(other, _CommitContext) and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(
            (
                self.data_node_id,
                self.reboot_times,
                self.topic_name,
                self.consumer_group_id,
                self.commit_id,
                self.seek_generation,
                self.region_id,
                self.physical_time,
                self.writer_id,
                self.writer_progress,
            )
        )


class SubscriptionSession(object):
    """SQL topic management and pull-consumer factory for one IoTDB cluster."""

    def __init__(
        self,
        node_urls=None,
        username=Session.DEFAULT_USER,
        password=Session.DEFAULT_PASSWORD,
        sql_dialect="tree",
        database=None,
        time_zone=Session.DEFAULT_ZONE_ID,
        enable_redirection=True,
        enable_compression=False,
        use_ssl=False,
        ca_certs=None,
        connection_timeout_in_ms=None,
        client_cert=None,
        client_key=None,
    ):
        self._config = {
            "node_urls": list(node_urls or ["localhost:6667"]),
            "username": username,
            "password": password,
            "sql_dialect": sql_dialect,
            "database": database,
            "time_zone": time_zone,
            "enable_redirection": enable_redirection,
            "enable_compression": enable_compression,
            "use_ssl": use_ssl,
            "ca_certs": ca_certs,
            "connection_timeout_in_ms": connection_timeout_in_ms,
            "client_cert": client_cert,
            "client_key": client_key,
        }
        self._session = None

    def open(self):
        if self._session is not None:
            return
        self._session = _open_session(self._config)

    def close(self):
        if self._session is not None:
            self._session.close()
            self._session = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def create_topic(self, topic_name, attributes=None, if_not_exists=False):
        self._require_open()
        topic_sql = _quote_identifier(topic_name)
        condition = " IF NOT EXISTS" if if_not_exists else ""
        sql = "CREATE TOPIC{} {}".format(condition, topic_sql)
        if attributes:
            pairs = []
            for key, value in attributes.items():
                pairs.append("'{}'='{}'".format(_escape_sql(key), _escape_sql(value)))
            sql += " WITH ({})".format(",".join(pairs))
        self._session.execute_non_query_statement(sql)

    def drop_topic(self, topic_name, if_exists=False):
        self._require_open()
        condition = " IF EXISTS" if if_exists else ""
        self._session.execute_non_query_statement(
            "DROP TOPIC{} {}".format(condition, _quote_identifier(topic_name))
        )

    def get_subscriptions(self, topic_name):
        """Return the subscriptions registered for one topic."""
        self._require_open()
        sql = "SHOW SUBSCRIPTIONS ON {}".format(_quote_identifier(topic_name))
        subscriptions = []
        with self._session.execute_query_statement(sql) as data_set:
            while data_set.has_next():
                fields = data_set.next().get_fields()
                if len(fields) != 4:
                    raise SubscriptionError(
                        "SHOW SUBSCRIPTIONS returned {} fields, expected 4.".format(
                            len(fields)
                        )
                    )
                values = [field.get_string_value() for field in fields]
                subscriptions.append(Subscription(*values))
        return subscriptions

    def drop_subscription_if_exists(self, subscription_id):
        """Remove a remote Subscription by its server-reported identifier."""
        self._require_open()
        self._session.execute_non_query_statement(
            "DROP SUBSCRIPTION IF EXISTS {}".format(_quote_identifier(subscription_id))
        )

    def create_pull_consumer(
        self,
        consumer_group_id=None,
        consumer_id=None,
        file_save_dir="iotdb-subscription",
        heartbeat_interval_ms=30000,
        thrift_max_frame_size=67108864,
        file_save_fsync=False,
        max_file_bytes=10 * 1024 * 1024 * 1024,
    ):
        self._require_open()
        return SubscriptionPullConsumer(
            self._config,
            consumer_group_id=consumer_group_id,
            consumer_id=consumer_id,
            file_save_dir=file_save_dir,
            heartbeat_interval_ms=heartbeat_interval_ms,
            thrift_max_frame_size=thrift_max_frame_size,
            file_save_fsync=file_save_fsync,
            max_file_bytes=max_file_bytes,
        )

    def _require_open(self):
        if self._session is None:
            raise IoTDBConnectionException("SubscriptionSession is not open.")


class SubscriptionPullConsumer(object):
    """Pull Subscription messages and persist snapshot TsFiles to local storage."""

    def __init__(
        self,
        session_config,
        consumer_group_id=None,
        consumer_id=None,
        file_save_dir="iotdb-subscription",
        heartbeat_interval_ms=30000,
        thrift_max_frame_size=67108864,
        file_save_fsync=False,
        max_file_bytes=10 * 1024 * 1024 * 1024,
    ):
        self._session_config = session_config
        self.consumer_group_id = consumer_group_id
        self.consumer_id = consumer_id
        self.consumer_instance_id = uuid.uuid4().hex
        self.file_save_dir = Path(file_save_dir)
        self.heartbeat_interval_ms = max(int(heartbeat_interval_ms), 1000)
        self.thrift_max_frame_size = int(thrift_max_frame_size)
        self.file_save_fsync = bool(file_save_fsync)
        self.max_file_bytes = int(max_file_bytes)
        if self.max_file_bytes < 1:
            raise ValueError("max_file_bytes must be positive")
        self._providers = {}
        self._topics = set()
        self.latest_watermark_timestamp = -(1 << 63)
        self._lock = threading.RLock()
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread = None
        self._open = False
        self._stopping = False
        self._pending_transfers = {}

    def open(self):
        if self._open:
            return
        self._stopping = False
        first_url = self._session_config["node_urls"][0]
        host, port = _parse_node_url(first_url)
        provider_session = _open_provider_session(self._session_config, host, port)
        self.thrift_max_frame_size = min(
            self.thrift_max_frame_size,
            _server_thrift_max_frame_size(provider_session, self.thrift_max_frame_size),
        )
        handshake_succeeded = False
        try:
            response = self._send(provider_session, 0, self._handshake_body())
            handshake_succeeded = True
            body = _first_body(response)
            if body is None:
                raise SubscriptionError("Subscription handshake returned no identity.")
            reader = io.BytesIO(body)
            data_node_id = _read_int(reader)
            self.consumer_id = _read_string(reader)
            self.consumer_group_id = _read_string(reader)
            self._providers[data_node_id] = (host, port, provider_session)

            self._open = True
            endpoints, topics_to_unsubscribe = self._heartbeat(provider_session)
            for topic_name in topics_to_unsubscribe:
                if topic_name in self._topics:
                    self.unsubscribe(topic_name)
            for node_id, endpoint in endpoints.items():
                if node_id != data_node_id:
                    try:
                        self._add_provider(node_id, endpoint)
                    except Exception:
                        continue
        except Exception:
            if handshake_succeeded:
                try:
                    self._send(provider_session, 4)
                except Exception:
                    pass
            provider_session.close()
            self.close()
            raise

        self._open = True
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="iotdb-subscription-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def close(self):
        self._stopping = True
        self._heartbeat_stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=2)
            self._heartbeat_thread = None
        with self._lock:
            if self._open:
                for _, _, session in list(self._providers.values()):
                    try:
                        self._send(session, 4)
                    except Exception:
                        pass
            for _, _, session in list(self._providers.values()):
                try:
                    session.close()
                except Exception:
                    pass
            self._providers.clear()
            self._topics.clear()
            self._pending_transfers.clear()
            self._open = False

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def subscribe(self, topic_name):
        self._require_open()
        body = _write_string_set([topic_name])
        subscribed = []
        with self._lock:
            try:
                for _, _, session in self._providers.values():
                    self._send(session, 5, body)
                    subscribed.append(session)
            except Exception:
                for session in subscribed:
                    try:
                        self._send(session, 6, body)
                    except Exception:
                        pass
                raise
            self._topics.add(topic_name)

    def unsubscribe(self, topic_name):
        self._require_open()
        body = _write_string_set([topic_name])
        with self._lock:
            for _, _, session in self._providers.values():
                self._send(session, 6, body)
            self._topics.discard(topic_name)

    def poll(self, timeout_ms=1000):
        """Return complete files received before the timeout expires."""
        self._require_open()
        deadline = time.monotonic() + max(int(timeout_ms), 0) / 1000.0
        messages = []
        with self._lock:
            messages.extend(self._resume_pending_transfers(deadline))
            if messages or not self._topics:
                return messages

            providers = list(self._providers.items())
            per_provider_timeout = max(
                1, int(max(timeout_ms, 0) / max(len(providers), 1))
            )
            for node_id, (_, _, session) in providers:
                remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
                if remaining_ms == 0:
                    break
                response = self._send(
                    session,
                    2,
                    _poll_body(
                        self._topics,
                        min(per_provider_timeout, remaining_ms),
                        self.thrift_max_frame_size,
                    ),
                )
                for item in _decode_poll_bodies(response):
                    response_type = item["type"]
                    if response_type == 2:
                        transfer = self._start_file_transfer(node_id, item)
                        message = self._continue_file_transfer(transfer, deadline)
                        if message is not None:
                            messages.append(message)
                    elif response_type == 5:
                        topic_name = item["context"].topic_name
                        if topic_name in self._topics:
                            self.unsubscribe(topic_name)
                    elif response_type == 7:
                        self.latest_watermark_timestamp = max(
                            self.latest_watermark_timestamp,
                            item["watermark_timestamp"],
                        )
                    elif response_type == 0 and item["critical"]:
                        raise SubscriptionError(
                            item["error"] or "Subscription poll failed."
                        )

                    if time.monotonic() >= deadline:
                        break
        return messages

    def commit(self, message):
        """Acknowledge a completed file after the caller has processed or moved it."""
        self._require_open()
        context = message._commit_context
        with self._lock:
            provider = self._providers.get(context.data_node_id)
            if provider is None:
                self._sync_providers()
                provider = self._providers.get(context.data_node_id)
            if provider is None:
                raise SubscriptionError(
                    "No active DataNode connection for commit context from node {}.".format(
                        context.data_node_id
                    )
                )
            body = struct.pack(">i", 1) + _write_commit_context(context) + b"\x00"
            response = self._send(provider[2], 3, body)
            commit_body = _first_body(response)
            if not commit_body:
                raise SubscriptionError(
                    "Subscription commit returned no acknowledgement."
                )
            reader = io.BytesIO(commit_body)
            accepted_count = _read_int(reader)
            accepted = [_read_commit_context(reader) for _ in range(accepted_count)]
            if context not in accepted:
                raise SubscriptionError("IoTDB did not accept the Subscription commit.")

    def all_topic_messages_have_been_consumed(self):
        return not self._topics

    def get_latest_watermark_timestamp(self):
        """Return the greatest watermark timestamp received, or the int64 minimum."""
        return self.latest_watermark_timestamp

    def _handshake_body(self):
        config = {
            "username": self._session_config["username"],
            "password": self._session_config["password"],
            "sql-dialect": self._session_config["sql_dialect"],
            "heartbeat-interval-ms": str(self.heartbeat_interval_ms),
            "connection-timeout-ms": str(
                self._session_config["connection_timeout_in_ms"]
                if self._session_config["connection_timeout_in_ms"] is not None
                else 10000
            ),
            "consumer-instance-id": self.consumer_instance_id,
        }
        if self.consumer_id is not None:
            config["consumer-id"] = self.consumer_id
        if self.consumer_group_id is not None:
            config["group-id"] = self.consumer_group_id
        return _write_string_map(config)

    def _send(self, session, request_type, body=None):
        request = TPipeSubscribeReq(version=1, type=request_type, body=body)
        try:
            response = _pipe_subscribe(session, request)
        except (
            TTransport.TTransportException,
            OSError,
            TimeoutError,
            IoTDBConnectionException,
        ):
            node_id = next(
                (
                    current_node_id
                    for current_node_id, provider in self._providers.items()
                    if provider[2] is session
                ),
                None,
            )
            if node_id is None or self._stopping:
                raise
            self._reconnect_provider(node_id)
            response = _pipe_subscribe(self._providers[node_id][2], request)
        rpc_utils.verify_success(response.status)
        return response

    def _reconnect_provider(self, node_id, endpoint=None):
        """Replace a dropped DataNode connection and restore its handshake and topics."""
        with self._lock:
            current = self._providers.pop(node_id, None)
            if endpoint is None and current is not None:
                endpoint = (current[0], current[1])
            if current is not None:
                try:
                    current[2].close()
                except Exception:
                    pass
            if endpoint is None:
                raise SubscriptionError(
                    "No endpoint is available for DataNode {} reconnection.".format(
                        node_id
                    )
                )

            host, port = endpoint
            session = _open_provider_session(self._session_config, host, port)
            self.thrift_max_frame_size = min(
                self.thrift_max_frame_size,
                _server_thrift_max_frame_size(session, self.thrift_max_frame_size),
            )
            handshake_succeeded = False
            try:
                response = self._send(session, 0, self._handshake_body())
                handshake_succeeded = True
                body = _first_body(response)
                if body is None:
                    raise SubscriptionError(
                        "Subscription reconnect returned no identity."
                    )
                reader = io.BytesIO(body)
                actual_node_id = _read_int(reader)
                actual_consumer_id = _read_string(reader)
                actual_group_id = _read_string(reader)
                if actual_node_id != node_id:
                    raise SubscriptionError(
                        "Subscription endpoint advertised node {} but connected to node {}.".format(
                            node_id, actual_node_id
                        )
                    )
                if (
                    actual_consumer_id != self.consumer_id
                    or actual_group_id != self.consumer_group_id
                ):
                    raise SubscriptionError(
                        "Subscription reconnect returned a different consumer identity."
                    )
                self._providers[node_id] = (host, port, session)
                for topic_name in self._topics:
                    self._send(session, 5, _write_string_set([topic_name]))
            except Exception:
                if handshake_succeeded:
                    try:
                        self._send(session, 4)
                    except Exception:
                        pass
                session.close()
                raise

    def _heartbeat(self, session):
        response = self._send(session, 1)
        return _decode_heartbeat_endpoints(response)

    def _heartbeat_loop(self):
        while not self._heartbeat_stop.wait(self.heartbeat_interval_ms / 1000.0):
            try:
                self._sync_providers()
            except Exception:
                # Poll and commit calls report errors to the application. A heartbeat failure is
                # retried on the next interval so a transient outage does not end the worker.
                continue

    def _sync_providers(self):
        with self._lock:
            endpoints = {}
            topics_to_unsubscribe = set()
            for _, _, session in list(self._providers.values()):
                current_endpoints, current_topics = self._heartbeat(session)
                endpoints.update(current_endpoints)
                topics_to_unsubscribe.update(current_topics)

            stale_node_ids = self._providers.keys() - endpoints.keys()
            for node_id in stale_node_ids:
                _, _, session = self._providers.pop(node_id)
                try:
                    session.close()
                except Exception:
                    pass

            for topic_name in topics_to_unsubscribe:
                if topic_name in self._topics:
                    self.unsubscribe(topic_name)
            for node_id, endpoint in endpoints.items():
                provider = self._providers.get(node_id)
                if provider is not None and provider[:2] != endpoint:
                    self._reconnect_provider(node_id, endpoint)
                elif provider is None:
                    try:
                        self._add_provider(node_id, endpoint)
                    except Exception:
                        continue

    def _add_provider(self, node_id, endpoint):
        host = endpoint[0]
        port = endpoint[1]
        session = _open_provider_session(self._session_config, host, port)
        self.thrift_max_frame_size = min(
            self.thrift_max_frame_size,
            _server_thrift_max_frame_size(session, self.thrift_max_frame_size),
        )
        handshake_succeeded = False
        try:
            response = self._send(session, 0, self._handshake_body())
            handshake_succeeded = True
            body = _first_body(response)
            if body is None:
                raise SubscriptionError("Subscription handshake returned no identity.")
            reader = io.BytesIO(body)
            actual_node_id = _read_int(reader)
            actual_consumer_id = _read_string(reader)
            actual_group_id = _read_string(reader)
            if actual_node_id != node_id:
                raise SubscriptionError(
                    "Subscription endpoint advertised node {} but connected to node {}.".format(
                        node_id, actual_node_id
                    )
                )
            if (
                actual_consumer_id != self.consumer_id
                or actual_group_id != self.consumer_group_id
            ):
                raise SubscriptionError(
                    "Subscription endpoint returned a different consumer identity."
                )
            for topic_name in self._topics:
                self._send(session, 5, _write_string_set([topic_name]))
            self._providers[node_id] = (host, port, session)
        except Exception:
            if handshake_succeeded:
                try:
                    self._send(session, 4)
                except Exception:
                    pass
            session.close()
            raise

    def _start_file_transfer(self, node_id, item):
        context = item["context"]
        topic = _safe_path_component(context.topic_name)
        group = _safe_path_component(context.consumer_group_id)
        consumer = _safe_path_component(self.consumer_id)
        file_name = _safe_path_component(item["file_name"])
        file_dir = self.file_save_dir / group / consumer / topic
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / file_name
        transfer = self._pending_transfers.get(context)
        if transfer is None:
            if file_path.exists():
                file_path = file_path.with_name(
                    "{}.{}".format(file_path.name, uuid.uuid4().hex[:16])
                )
            transfer = {
                "node_id": node_id,
                "context": context,
                "file_name": item["file_name"],
                "path": file_path,
                "offset": 0,
            }
            self._pending_transfers[context] = transfer
        return transfer

    def _resume_pending_transfers(self, deadline):
        completed = []
        for transfer in list(self._pending_transfers.values()):
            message = self._continue_file_transfer(transfer, deadline)
            if message is not None:
                completed.append(message)
            if time.monotonic() >= deadline:
                break
        return completed

    def _continue_file_transfer(self, transfer, deadline):
        provider = self._providers.get(transfer["node_id"])
        if provider is None:
            self._sync_providers()
            provider = self._providers.get(transfer["node_id"])
        if provider is None:
            return None
        path = transfer["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        mode = "r+b" if path.exists() else "w+b"
        with path.open(mode) as output:
            file_size = path.stat().st_size
            if (
                file_size > self.max_file_bytes
                or transfer["offset"] > self.max_file_bytes
            ):
                raise SubscriptionError(
                    "Subscription TsFile exceeds the configured size limit."
                )
            if file_size != transfer["offset"]:
                if file_size < transfer["offset"]:
                    transfer["offset"] = file_size
                else:
                    output.truncate(transfer["offset"])
            output.seek(transfer["offset"])
            while time.monotonic() < deadline:
                provider = self._providers.get(transfer["node_id"])
                if provider is None:
                    self._sync_providers()
                    provider = self._providers.get(transfer["node_id"])
                if provider is None:
                    return None
                timeout = max(1, int((deadline - time.monotonic()) * 1000))
                request_body = _poll_file_body(
                    transfer["context"],
                    transfer["offset"],
                    timeout,
                    self.thrift_max_frame_size,
                )
                response = self._send(provider[2], 2, request_body)
                items = _decode_poll_bodies(response)
                if not items:
                    return None
                item = items[0]
                if item["context"] != transfer["context"]:
                    raise SubscriptionError(
                        "File response commit context changed mid-transfer."
                    )
                if item["type"] == 3:
                    if item["file_name"] != transfer["file_name"]:
                        raise SubscriptionError(
                            "File response name changed mid-transfer."
                        )
                    next_offset = transfer["offset"] + len(item["piece"])
                    if next_offset != item["next_offset"]:
                        raise SubscriptionError(
                            "Subscription file piece offset is inconsistent."
                        )
                    if next_offset > self.max_file_bytes:
                        raise SubscriptionError(
                            "Subscription TsFile exceeds the configured size limit."
                        )
                    written = output.write(item["piece"])
                    if written != len(item["piece"]):
                        raise SubscriptionError(
                            "Could not write the complete TsFile piece."
                        )
                    output.flush()
                    if self.file_save_fsync:
                        os.fsync(output.fileno())
                    transfer["offset"] = next_offset
                elif item["type"] == 4:
                    if item["file_name"] != transfer["file_name"]:
                        raise SubscriptionError("File seal name changed mid-transfer.")
                    if item["file_length"] > self.max_file_bytes:
                        raise SubscriptionError(
                            "Subscription TsFile exceeds the configured size limit."
                        )
                    if transfer["offset"] != item["file_length"]:
                        raise SubscriptionError(
                            "Subscription file length does not match its seal."
                        )
                    self._pending_transfers.pop(transfer["context"], None)
                    return SubscriptionMessage(
                        path,
                        transfer["context"].topic_name,
                        item["database_name"],
                        transfer["context"],
                    )
                elif item["type"] == 0 and item["critical"]:
                    raise SubscriptionError(
                        item["error"] or "Subscription file poll failed."
                    )
                elif item["type"] == 0:
                    return None
        return None

    def _require_open(self):
        if not self._open:
            raise IoTDBConnectionException("SubscriptionPullConsumer is not open.")


def _open_session(config):
    session = Session.init_from_node_urls(
        config["node_urls"],
        config["username"],
        config["password"],
        zone_id=config["time_zone"],
        enable_redirection=config["enable_redirection"],
        use_ssl=config["use_ssl"],
        ca_certs=config["ca_certs"],
        connection_timeout_in_ms=config["connection_timeout_in_ms"],
        client_cert=config["client_cert"],
        client_key=config["client_key"],
    )
    session.sql_dialect = config["sql_dialect"]
    session.database = config["database"]
    session.open(config["enable_compression"])
    return session


def _open_provider_session(config, host, port):
    provider_config = dict(config)
    if ":" in host:
        provider_config["node_urls"] = ["[{}]:{}".format(host, port)]
    else:
        provider_config["node_urls"] = ["{}:{}".format(host, port)]
    provider_config["enable_redirection"] = False
    return _open_session(provider_config)


def _quote_identifier(value):
    if not value or not isinstance(value, str):
        raise ValueError("topic_name must be a non-empty string")
    return "`{}`".format(value.replace("`", "``"))


def _escape_sql(value):
    return str(value).replace("'", "''")


def _parse_node_url(url):
    if url.startswith("["):
        close = url.find("]")
        if close < 0 or url[close + 1 : close + 2] != ":":
            raise ValueError("Invalid node URL: {}".format(url))
        return url[1:close], int(url[close + 2 :])
    host, port = url.rsplit(":", 1)
    return host, int(port)


def _safe_path_component(value):
    if value is None or value == "":
        raise SubscriptionError("Subscription path component must not be empty.")
    value = str(value)
    if value in (".", ".."):
        return value.replace(".", "%2E")
    return quote(value, safe="-_.~")


def _write_string(value):
    if value is None:
        return struct.pack(">i", -1)
    encoded = str(value).encode("utf-8")
    return struct.pack(">i", len(encoded)) + encoded


def _write_string_map(values):
    body = [struct.pack(">i", len(values))]
    for key, value in values.items():
        body.append(_write_string(key))
        body.append(_write_string(value))
    return b"".join(body)


def _write_string_set(values):
    body = [struct.pack(">i", len(values))]
    body.extend(b"\x06" + _write_string(value) for value in values)
    return b"".join(body)


def _write_commit_context(context):
    body = [
        struct.pack(">bii", 2, context.data_node_id, context.reboot_times),
        _write_string(context.topic_name),
        _write_string(context.consumer_group_id),
        struct.pack(">qq", context.commit_id, context.seek_generation),
        _write_string(context.region_id),
        struct.pack(">qb", context.physical_time, context.writer_id is not None),
    ]
    if context.writer_id is not None:
        region_id, node_id = context.writer_id
        physical_time, local_seq = context.writer_progress
        body.extend(
            [
                _write_string(region_id),
                struct.pack(">iqq", node_id, physical_time, local_seq),
            ]
        )
    return b"".join(body)


def _poll_body(topic_names, timeout_ms, max_bytes):
    return b"".join(
        [
            struct.pack(">h", 0),
            _write_string_set(sorted(topic_names)),
            struct.pack(">qqi", timeout_ms, max_bytes, 0),
        ]
    )


def _poll_file_body(context, writing_offset, timeout_ms, max_bytes):
    return b"".join(
        [
            struct.pack(">h", 1),
            _write_commit_context(context),
            struct.pack(">qqqi", writing_offset, timeout_ms, max_bytes, 0),
        ]
    )


def _first_body(response):
    if not response.body:
        return None
    body = response.body[0]
    if isinstance(body, memoryview):
        return body.tobytes()
    return bytes(body)


def _decode_poll_bodies(response):
    items = []
    for body in response.body or []:
        if body:
            items.append(_decode_poll_response(bytes(body)))
    return items


def _decode_poll_response(body):
    reader = io.BytesIO(body)
    response_type = _read_short(reader)
    item = {"type": response_type}
    if response_type == 0:
        item["error"] = _read_string(reader)
        item["critical"] = _read_bool(reader)
    elif response_type == 2:
        item["file_name"] = _read_string(reader)
    elif response_type == 3:
        item["file_name"] = _read_string(reader)
        item["next_offset"] = _read_long(reader)
        piece_length = _read_int(reader)
        item["piece"] = reader.read(piece_length)
        if len(item["piece"]) != piece_length:
            raise SubscriptionError("Truncated Subscription TsFile piece.")
    elif response_type == 4:
        item["file_name"] = _read_string(reader)
        item["file_length"] = _read_long(reader)
        item["database_name"] = _read_string(reader)
    elif response_type == 7:
        item["watermark_timestamp"] = _read_long(reader)
        item["data_node_id"] = _read_int(reader)
    elif response_type not in (5,):
        raise SubscriptionError(
            "Unsupported Subscription response type {}.".format(response_type)
        )
    item["context"] = _read_commit_context(reader)
    if reader.tell() < len(body):
        item["time_selected"] = _read_bool(reader)
    return item


def _decode_commit_contexts(body):
    if not body:
        return []
    reader = io.BytesIO(body)
    count = _read_int(reader)
    return [_read_commit_context(reader) for _ in range(count)]


def _decode_heartbeat_endpoints(response):
    body = _first_body(response)
    if not body:
        return {}, []
    reader = io.BytesIO(body)
    topic_count = _read_int(reader)
    for _ in range(topic_count):
        _read_string(reader)
        _read_string_map(reader)
    endpoint_count = _read_int(reader)
    endpoints = {}
    for _ in range(endpoint_count):
        node_id = _read_int(reader)
        host = _read_string(reader)
        port = _read_int(reader)
        endpoints[node_id] = (host, port)
    return endpoints, _read_string_list(reader)


def _read_commit_context(reader):
    version = _read_byte(reader)
    if version != 2:
        raise SubscriptionError(
            "Unsupported Subscription commit context version {}.".format(version)
        )
    data_node_id = _read_int(reader)
    reboot_times = _read_int(reader)
    topic_name = _read_string(reader)
    consumer_group_id = _read_string(reader)
    commit_id = _read_long(reader)
    seek_generation = _read_long(reader)
    region_id = _read_string(reader)
    physical_time = _read_long(reader)
    has_writer_progress = _read_byte(reader) != 0
    writer_id = None
    writer_progress = None
    if has_writer_progress:
        writer_region_id = _read_string(reader)
        writer_node_id = _read_int(reader)
        writer_physical_time = _read_long(reader)
        writer_local_seq = _read_long(reader)
        writer_id = (writer_region_id, writer_node_id)
        writer_progress = (writer_physical_time, writer_local_seq)
    return _CommitContext(
        data_node_id,
        reboot_times,
        topic_name,
        consumer_group_id,
        commit_id,
        seek_generation,
        region_id,
        physical_time,
        writer_id,
        writer_progress,
    )


def _read_string_map(reader):
    count = _read_int(reader)
    return {_read_string(reader): _read_string(reader) for _ in range(count)}


def _read_string_list(reader):
    count = _read_int(reader)
    return [_read_string(reader) for _ in range(count)]


def _read_string(reader):
    length = _read_int(reader)
    if length < 0:
        return None
    value = reader.read(length)
    if len(value) != length:
        raise SubscriptionError("Truncated string in Subscription response.")
    return value.decode("utf-8")


def _read_byte(reader):
    data = reader.read(1)
    if len(data) != 1:
        raise SubscriptionError("Truncated byte in Subscription response.")
    return struct.unpack(">b", data)[0]


def _read_bool(reader):
    return _read_byte(reader) != 0


def _read_short(reader):
    return _read_number(reader, ">h")


def _read_int(reader):
    return _read_number(reader, ">i")


def _read_long(reader):
    return _read_number(reader, ">q")


def _read_number(reader, fmt):
    size = struct.calcsize(fmt)
    data = reader.read(size)
    if len(data) != size:
        raise SubscriptionError("Truncated number in Subscription response.")
    return struct.unpack(fmt, data)[0]


def _pipe_subscribe(session, request):
    """Send Subscription RPCs through the pinned apache-iotdb Session client."""
    method = getattr(session, "_pipe_subscribe", None)
    if method is not None:
        return method(request)
    client = getattr(session, "_Session__client", None)
    if client is None:
        raise IoTDBConnectionException("IoTDB Session is not open.")
    return client.pipeSubscribe(request)


def _server_thrift_max_frame_size(session, fallback):
    """Read the negotiated frame ceiling when the pinned client exposes it."""
    method = getattr(session, "_get_server_thrift_max_frame_size", None)
    if method is not None:
        return method(fallback)
    client = getattr(session, "_Session__client", None)
    if client is None:
        return fallback
    try:
        properties = client.getProperties()
    except TTransport.TException:
        return fallback
    server_limit = getattr(properties, "thriftMaxFrameSize", None)
    return server_limit if server_limit and server_limit > 0 else fallback
