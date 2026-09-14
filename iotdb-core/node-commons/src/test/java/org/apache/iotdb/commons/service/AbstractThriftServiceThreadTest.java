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

import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.junit.Assert;
import org.junit.Test;

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

  private static byte[] binaryListHeader(int size) {
    return new byte[] {
      TType.BYTE, (byte) (size >>> 24), (byte) (size >>> 16), (byte) (size >>> 8), (byte) size, 0, 0
    };
  }

  private static byte[] compactListHeader(int size) {
    return new byte[] {(byte) 0xF3, (byte) size, 0, 0};
  }
}
