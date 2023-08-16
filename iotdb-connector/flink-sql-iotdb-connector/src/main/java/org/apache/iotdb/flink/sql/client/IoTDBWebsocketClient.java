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
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;

public class IoTDBWebsocketClient extends WebSocketClient {
  private IoTDBCDCSourceFunction function;
  private SourceContext ctx;

  public IoTDBWebsocketClient(URI uri, IoTDBCDCSourceFunction function) {
    super(uri);
    this.function = function;
  }

  @Override
  public void onOpen(ServerHandshake serverHandshake) {}

  @Override
  public void onMessage(String s) {}

  @Override
  public void onMessage(ByteBuffer bytes) {
    super.onMessage(bytes);
    long commitId = bytes.getLong();
    byte[] tabletBytes = new byte[bytes.capacity() - Long.BYTES];
    ByteBuffer tabletBuffer = bytes.get(tabletBytes, Long.BYTES, bytes.capacity() - Long.BYTES);
    Tablet tablet = Tablet.deserialize(tabletBuffer);
    function.collectTablet(tablet, ctx);
    this.send(String.format("ACK:%d", commitId));
  }

  @Override
  public void onClose(int i, String s, boolean b) {}

  @Override
  public void onError(Exception e) {
    e.printStackTrace();
    throw new RuntimeException(e);
  }

  public void setContext(SourceContext ctx) {
    this.ctx = ctx;
  }
}
