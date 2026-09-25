/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.rpc;

import org.apache.iotdb.rpc.i18n.RpcMessages;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@RunWith(Parameterized.class)
public class TElasticFramedTransportReadTest {
  private static final int MAX_FRAME_SIZE = 1024 * 1024;
  private static final int ELEMENT_COUNT = 200_001;

  private final boolean compact;
  private final boolean copyBinary;
  private final boolean snappy;

  @Parameterized.Parameters(name = "compact={0}, copyBinary={1}, snappy={2}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> parameters = new ArrayList<>();
    for (boolean compact : new boolean[] {false, true}) {
      for (boolean copyBinary : new boolean[] {false, true}) {
        for (boolean snappy : new boolean[] {false, true}) {
          parameters.add(new Object[] {compact, copyBinary, snappy});
        }
      }
    }
    return parameters;
  }

  public TElasticFramedTransportReadTest(boolean compact, boolean copyBinary, boolean snappy) {
    this.compact = compact;
    this.copyBinary = copyBinary;
    this.snappy = snappy;
  }

  @Test
  public void testTruncatedRequestRejectedBeforeContainerAllocation() throws Exception {
    byte[] frame =
        serialize(
            p -> {
              p.writeMessageBegin(new TMessage("insertRecord", TMessageType.CALL, 1));
              p.writeStructBegin(new TStruct("insertRecord_args"));
              p.writeFieldBegin(new TField("req", TType.STRUCT, (short) 1));
              p.writeStructBegin(new TStruct("TSInsertRecordReq"));
              p.writeFieldBegin(new TField("columnCategoryies", TType.LIST, (short) 8));
              p.writeListBegin(new TList(TType.BYTE, ELEMENT_COUNT));
              // Flush a complete frame that ends at the container header, without any elements.
            });
    try (TElasticFramedTransport transport = transport(new TMemoryInputTransport(frame))) {
      TProtocol protocol = protocol(transport);
      assertEquals("insertRecord", protocol.readMessageBegin().name);
      IClientRPCService.insertRecord_args args = new IClientRPCService.insertRecord_args();
      TTransportException exception =
          assertThrows(TTransportException.class, () -> args.read(protocol));
      assertNotNull(args.req);
      assertNull(args.req.columnCategoryies);
      assertInsufficientFrame(exception, ELEMENT_COUNT, 0);
    }
  }

  @Test
  public void testLargeCompleteRequestsInSuccessiveFrames() throws Exception {
    TSInsertRecordReq large =
        new TSInsertRecordReq(1, "root.sg.d", Collections.emptyList(), ByteBuffer.allocate(0), 1)
            .setColumnCategoryies(Collections.nCopies(ELEMENT_COUNT, (byte) 0));
    TSInsertRecordReq empty =
        new TSInsertRecordReq(1, "root.sg.d", Collections.emptyList(), ByteBuffer.allocate(0), 2)
            .setColumnCategoryies(Collections.emptyList());
    TMemoryBuffer wire = new TMemoryBuffer(128);
    try (TElasticFramedTransport output = transport(wire)) {
      IClientRPCService.Client client = new IClientRPCService.Client(protocol(output));
      client.send_insertRecord(large);
      client.send_insertRecord(empty);
    }
    try (TElasticFramedTransport input =
        transport(new TMemoryInputTransport(Arrays.copyOf(wire.getArray(), wire.length())))) {
      TProtocol protocol = protocol(input);
      for (TSInsertRecordReq expected : new TSInsertRecordReq[] {large, empty}) {
        assertEquals("insertRecord", protocol.readMessageBegin().name);
        IClientRPCService.insertRecord_args args = new IClientRPCService.insertRecord_args();
        args.read(protocol);
        protocol.readMessageEnd();
        assertEquals(expected, args.req);
      }
    }
  }

  @Test
  public void testTruncatedContainerHeaders() throws Exception {
    for (byte elementType : new byte[] {TType.BYTE, TType.STRUCT, TType.LIST, TType.MAP}) {
      for (byte containerType : new byte[] {TType.LIST, TType.SET, TType.MAP}) {
        byte[] frame =
            serialize(
                p -> {
                  if (containerType == TType.LIST) {
                    p.writeListBegin(new TList(elementType, ELEMENT_COUNT));
                  } else if (containerType == TType.SET) {
                    p.writeSetBegin(new TSet(elementType, ELEMENT_COUNT));
                  } else {
                    p.writeMapBegin(new TMap(TType.BYTE, elementType, ELEMENT_COUNT));
                  }
                });
        try (TElasticFramedTransport input = transport(new TMemoryInputTransport(frame))) {
          TProtocol p = protocol(input);
          long minimumBytes =
              (long) ELEMENT_COUNT
                  * (p.getMinSerializedSize(elementType) + (containerType == TType.MAP ? 1 : 0));
          TTransportException exception =
              assertThrows(
                  TTransportException.class,
                  () -> {
                    if (containerType == TType.LIST) {
                      p.readListBegin();
                    } else if (containerType == TType.SET) {
                      p.readSetBegin();
                    } else {
                      p.readMapBegin();
                    }
                  });
          assertInsufficientFrame(exception, minimumBytes, 0);
        }
      }
    }
  }

  @Test
  public void testCompleteBinaryAtFrameBoundary() throws Exception {
    ByteBuffer binary = ByteBuffer.wrap(new byte[ELEMENT_COUNT]);
    byte[] frame =
        serialize(
            p -> {
              p.writeString("");
              p.writeBinary(binary);
            });
    try (TElasticFramedTransport input = transport(new TMemoryInputTransport(frame))) {
      TProtocol protocol = protocol(input);
      assertEquals("", protocol.readString());
      assertEquals(binary, protocol.readBinary());
      input.checkReadBytesAvailable(0);
    }
  }

  @Test
  public void testTruncatedStringAndBinary() throws Exception {
    TMemoryBuffer buffer = new TMemoryBuffer(128);
    protocol(buffer).writeBinary(ByteBuffer.wrap(new byte[ELEMENT_COUNT]));
    int headerLength = buffer.length() - ELEMENT_COUNT;
    byte[] frame = serialize(p -> p.getTransport().write(buffer.getArray(), 0, headerLength));
    for (boolean binary : new boolean[] {false, true}) {
      try (TElasticFramedTransport input = transport(new TMemoryInputTransport(frame))) {
        TProtocol p = protocol(input);
        TTransportException exception =
            assertThrows(
                TTransportException.class,
                () -> {
                  if (binary) {
                    p.readBinary();
                  } else {
                    p.readString();
                  }
                });
        assertInsufficientFrame(exception, ELEMENT_COUNT, 0);
      }
    }
  }

  @Test
  public void testExistingMaximumReadSizeProtection() throws Exception {
    try (TElasticFramedTransport input = transport(new TMemoryInputTransport(new byte[0]))) {
      TTransportException exception =
          assertThrows(
              TTransportException.class, () -> input.checkReadBytesAvailable(MAX_FRAME_SIZE));
      assertEquals(TTransportException.CORRUPTED_DATA, exception.getType());
      assertEquals(
          String.format(
              RpcMessages.FRAME_ERROR_STRING_LENGTH_EXCEEDED, MAX_FRAME_SIZE, MAX_FRAME_SIZE, ""),
          exception.getMessage());
    }
  }

  private void assertInsufficientFrame(
      TTransportException exception, long required, int remaining) {
    assertEquals(TTransportException.CORRUPTED_DATA, exception.getType());
    assertEquals(
        String.format(
            RpcMessages
                .EXCEPTION_REQUIRED_READ_SIZE_ARG_EXCEEDS_REMAINING_FRAME_SIZE_ARG_ARG_9C0541EE,
            required,
            remaining,
            ""),
        exception.getMessage());
  }

  private TElasticFramedTransport transport(TTransport underlying) throws TTransportException {
    return snappy
        ? new TSnappyElasticFramedTransport(underlying, 128, MAX_FRAME_SIZE, copyBinary)
        : new TElasticFramedTransport(underlying, 128, MAX_FRAME_SIZE, copyBinary);
  }

  private TProtocol protocol(TTransport transport) {
    return compact ? new TCompactProtocol(transport) : new TBinaryProtocol(transport);
  }

  private byte[] serialize(ProtocolWriter writer) throws TException {
    TMemoryBuffer wire = new TMemoryBuffer(128);
    try (TElasticFramedTransport output = transport(wire)) {
      writer.write(protocol(output));
      output.flush();
    }
    return Arrays.copyOf(wire.getArray(), wire.length());
  }

  @FunctionalInterface
  private interface ProtocolWriter {
    void write(TProtocol protocol) throws TException;
  }
}
