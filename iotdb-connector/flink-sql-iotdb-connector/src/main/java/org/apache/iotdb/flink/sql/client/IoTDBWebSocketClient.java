/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.flink.sql.client;

import org.apache.iotdb.flink.sql.function.IoTDBCDCSourceFunction;
import org.apache.iotdb.flink.sql.wrapper.TabletWrapper;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;

public class IoTDBWebSocketClient extends WebSocketClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBWebSocketClient.class);
  private final IoTDBCDCSourceFunction function;

  public IoTDBWebSocketClient(URI uri, IoTDBCDCSourceFunction function) {
    super(uri);
    this.function = function;
  }

  @Override
  public void onOpen(ServerHandshake serverHandshake) {
    String log =
        String.format("The connection with %s:%d has been created.", uri.getHost(), uri.getPort());
    LOGGER.info(log);
  }

  @Override
  public void onMessage(String s) {
    // Do nothing
  }

  @Override
  public void onMessage(ByteBuffer bytes) {
    super.onMessage(bytes);
    long commitId = bytes.getLong();
    Tablet tablet = Tablet.deserialize(bytes);
    function.addTabletWrapper(new TabletWrapper(commitId, this, tablet));
  }

  @Override
  public void onClose(int i, String s, boolean b) {
    LOGGER.info("The connection to {}:{} has been closed.", uri.getHost(), uri.getPort());
  }

  @Override
  public void onError(Exception e) {
    String log =
        String.format(
            "An error occurred when connecting to %s:%s: %s.",
            uri.getHost(), uri.getPort(), e.getMessage());
    LOGGER.error(log);
  }
}
