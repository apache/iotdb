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

package org.apache.iotdb.commons.pipe.sink.payload.thrift;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Consumer;

public final class PipeTransferReqTestUtils {

  public static void assertVersionAndType(
      final TPipeTransferReq req,
      final IoTDBSinkRequestVersion expectedVersion,
      final PipeRequestType expectedType) {
    Assert.assertEquals(expectedVersion.getVersion(), req.version);
    Assert.assertEquals(expectedType.getType(), req.type);
  }

  public static void assertAirGapReqBytes(
      final byte[] reqBytes,
      final IoTDBSinkRequestVersion expectedVersion,
      final PipeRequestType expectedType,
      final Consumer<ByteBuffer> bodyAssertion) {
    final ByteBuffer reqBuffer = ByteBuffer.wrap(reqBytes);
    Assert.assertEquals(expectedVersion.getVersion(), ReadWriteIOUtils.readByte(reqBuffer));
    Assert.assertEquals(expectedType.getType(), ReadWriteIOUtils.readShort(reqBuffer));
    bodyAssertion.accept(reqBuffer);
    Assert.assertFalse(reqBuffer.hasRemaining());
  }

  public static TPipeTransferReq copyOf(final TPipeTransferReq req) {
    final TPipeTransferReq copy = new TPipeTransferReq();
    copy.version = req.version;
    copy.type = req.type;
    copy.body = req.body.duplicate();
    return copy;
  }

  public static byte[] toTransferReqBytes(final TPipeTransferReq req) {
    return BytesUtils.concatByteArrayList(
        Arrays.asList(new byte[] {req.version}, BytesUtils.shortToBytes(req.type), req.getBody()));
  }

  public static TPipeTransferReq readTransferReqFrom(final byte[] reqBytes) {
    final ByteBuffer reqBuffer = ByteBuffer.wrap(reqBytes);
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = ReadWriteIOUtils.readByte(reqBuffer);
    req.type = ReadWriteIOUtils.readShort(reqBuffer);
    req.body = reqBuffer.slice();
    return req;
  }

  private PipeTransferReqTestUtils() {
    // Utility class
  }
}
