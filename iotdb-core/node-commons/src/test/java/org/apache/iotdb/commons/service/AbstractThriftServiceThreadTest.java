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

package org.apache.iotdb.commons.service;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class AbstractThriftServiceThreadTest {

  @Test
  public void testProtocolFactoriesLimitContainerLength() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    int originalLimit = config.getThriftContainerLengthLimit();
    try {
      config.setThriftContainerLengthLimit(1);

      Assert.assertThrows(
          TProtocolException.class,
          () ->
              AbstractThriftServiceThread.getProtocolFactory(false)
                  .getProtocol(new TMemoryInputTransport(binaryListHeader(2)))
                  .readListBegin());
      Assert.assertThrows(
          TProtocolException.class,
          () ->
              AbstractThriftServiceThread.getProtocolFactory(true)
                  .getProtocol(new TMemoryInputTransport(compactListHeader(2)))
                  .readListBegin());
    } finally {
      config.setThriftContainerLengthLimit(originalLimit);
    }
  }

  @Test
  public void testGeneratedRequestRejectsOversizedByteList() throws TException {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    int originalLimit = config.getThriftContainerLengthLimit();
    try {
      config.setThriftContainerLengthLimit(1);

      Assert.assertThrows(
          TProtocolException.class,
          () ->
              new TSInsertRecordReq()
                  .read(
                      AbstractThriftServiceThread.getProtocolFactory(false)
                          .getProtocol(new TMemoryInputTransport(serializeRequest(false)))));
      Assert.assertThrows(
          TProtocolException.class,
          () ->
              new TSInsertRecordReq()
                  .read(
                      AbstractThriftServiceThread.getProtocolFactory(true)
                          .getProtocol(new TMemoryInputTransport(serializeRequest(true)))));
    } finally {
      config.setThriftContainerLengthLimit(originalLimit);
    }
  }

  private static byte[] serializeRequest(boolean compact) throws TException {
    TSInsertRecordReq request =
        new TSInsertRecordReq(1, "root.sg.d", Collections.emptyList(), ByteBuffer.allocate(0), 1)
            .setColumnCategoryies(Arrays.asList((byte) 0, (byte) 1));
    TMemoryBuffer buffer = new TMemoryBuffer(128);
    TProtocol protocol = compact ? new TCompactProtocol(buffer) : new TBinaryProtocol(buffer);
    request.write(protocol);
    return Arrays.copyOf(buffer.getArray(), buffer.length());
  }

  private static byte[] binaryListHeader(int size) {
    return new byte[] {
      TType.BYTE, (byte) (size >>> 24), (byte) (size >>> 16), (byte) (size >>> 8), (byte) size, 0, 0
    };
  }

  private static byte[] compactListHeader(int size) {
    return new byte[] {(byte) 0xF3, (byte) size, 0, 0};
  }
}
