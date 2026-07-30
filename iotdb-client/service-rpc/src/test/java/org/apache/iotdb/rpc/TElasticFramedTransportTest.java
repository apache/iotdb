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

    try {
      TElasticFramedTransport transport =
          new TElasticFramedTransport(
              new TByteBuffer(ByteBuffer.wrap(getTypicalTLSClientHelloByteArray())),
              128 * 1024 * 1024,
              512 * 1024 * 1024,
              false);
      transport.open();
      transport.read(ByteBuffer.allocate(4096));
      fail("Exception expected");
    } catch (TTransportException e) {
      assertEquals(
          "Singular frame size (369296129) detected, you may be sending TLS ClientHello requests to the Non-SSL Thrift-RPC port, please confirm that you are using the right configuration",
          e.getMessage());
    }
  }

  private static byte[] getTypicalTLSClientHelloByteArray() {
    String clientHelloHex =
        "16030301B3010001AF0303CEC349A4962AFCE0390D4E33D24050D1BF6B1CA63B190A25"
            + "BCFB83D87A3E352C20187B978A0EB2F554EC0E41A4CA34B850B2CE472EAB7B3F58443DE7CDBE901412004A13"
            + "0213011303C02CC02BCCA9C030CCA8C02F009FCCAA00A3009E00A2C024C028C023C027006B006A00670040C0"
            + "0AC014C009C0130039003800330032009D009C003D003C0035002F00FF0100011C000500050100000000000A"
            + "00160014001D001700180019001E01000101010201030104000B000201000011000900070200040000000000"
            + "17000000230000000D002C002A040305030603080708080804080508060809080A080B040105010601040203"
            + "0303010302020302010202002B00050403040303002D000201010032002C002A040305030603080708080804"
            + "080508060809080A080B04010501060104020303030103020203020102020033006B0069001D002097B98B24"
            + "B9A97EB7C913BDB8B363E79C9D47935264B2CF83BF422571FBD41C360017004104FC839279D372DCB60680D2"
            + "81B3DC8D3B88F6231A880A3650FD45322A79C9EA14CE073C0B71FC0AF9683BFC6DA95EB23B4122EC9E09EB7F"
            + "88FF565415DDF44367";
    byte[] bytes = new byte[clientHelloHex.length() / 2];
    for (int i = 0; i < clientHelloHex.length(); i += 2) {
      int value = Integer.parseInt(clientHelloHex.substring(i, i + 2), 16);
      bytes[i / 2] = (byte) value;
    }
    return bytes;
  }
}
