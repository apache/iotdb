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

package org.apache.iotdb.rpc;

import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TElasticFramedTransportTest {

  @Test
  public void testSingularSize() {

    try {
      TElasticFramedTransport transport =
          new TElasticFramedTransport(
              new TByteBuffer(
                  ByteBuffer.wrap("GET 127.0.0.1 HTTP/1.1".getBytes(StandardCharsets.UTF_8))),
              128 * 1024 * 1024,
              512 * 1024 * 1024,
              false);
      transport.open();
      transport.read(ByteBuffer.allocate(4096));
      fail("Exception expected");
    } catch (TTransportException e) {
      assertEquals(
          "Singular frame size (1195725856) detected, you may be sending HTTP GET/POST requests to the Thrift-RPC port, please confirm that you are using the right port",
          e.getMessage());
    }

    try {
      TElasticFramedTransport transport =
          new TElasticFramedTransport(
              new TByteBuffer(
                  ByteBuffer.wrap("POST 127.0.0.1 HTTP/1.1".getBytes(StandardCharsets.UTF_8))),
              128 * 1024 * 1024,
              512 * 1024 * 1024,
              false);
      transport.open();
      transport.read(ByteBuffer.allocate(4096));
      fail("Exception expected");
    } catch (TTransportException e) {
      assertEquals(
          "Singular frame size (1347375956) detected, you may be sending HTTP GET/POST requests to the Thrift-RPC port, please confirm that you are using the right port",
          e.getMessage());
    }
  }
}
