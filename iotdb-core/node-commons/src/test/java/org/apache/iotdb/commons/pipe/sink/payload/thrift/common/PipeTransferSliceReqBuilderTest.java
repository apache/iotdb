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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.common;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeTransferSliceReqBuilderTest {

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private int originalRequestSliceThresholdBytes;

  @Before
  public void setUp() {
    originalRequestSliceThresholdBytes = commonConfig.getPipeSinkRequestSliceThresholdBytes();
    commonConfig.setPipeSinkRequestSliceThresholdBytes(4);
  }

  @After
  public void tearDown() {
    commonConfig.setPipeSinkRequestSliceThresholdBytes(originalRequestSliceThresholdBytes);
  }

  @Test
  public void testBuildSliceReq() throws Exception {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 10);
    final int bodySizeLimit = PipeTransferSliceReqBuilder.getBodySizeLimit();

    Assert.assertTrue(PipeTransferSliceReqBuilder.shouldSlice(req, bodySizeLimit));
    Assert.assertEquals(3, PipeTransferSliceReqBuilder.getSliceCount(req, bodySizeLimit));

    final byte[][] expectedBodies = {
      new byte[] {0, 1, 2, 3}, new byte[] {4, 5, 6, 7}, new byte[] {8, 9}
    };
    for (int i = 0; i < expectedBodies.length; i++) {
      final PipeTransferSliceReq slice =
          PipeTransferSliceReqBuilder.buildSliceReq(req, 123, i, 3, bodySizeLimit);
      assertSlice(slice, 123, req.getType(), 10, expectedBodies[i], i, 3);
      assertSlice(
          PipeTransferSliceReq.fromTPipeTransferReq(copyOf(slice)),
          123,
          req.getType(),
          10,
          expectedBodies[i],
          i,
          3);
    }
    Assert.assertEquals(0, req.body.position());
  }

  @Test
  public void testShouldSliceOnlyForVersion1RequestsAboveThreshold() {
    final int bodySizeLimit = PipeTransferSliceReqBuilder.getBodySizeLimit();

    Assert.assertFalse(
        PipeTransferSliceReqBuilder.shouldSlice(
            createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 3), bodySizeLimit));
    Assert.assertFalse(
        PipeTransferSliceReqBuilder.shouldSlice(
            createReq((byte) (IoTDBSinkRequestVersion.VERSION_1.getVersion() + 1), 10),
            bodySizeLimit));
    Assert.assertTrue(
        PipeTransferSliceReqBuilder.shouldSlice(
            createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 4), bodySizeLimit));
  }

  @Test
  public void testGetSliceCountForBoundaryBodySizes() {
    final int bodySizeLimit = PipeTransferSliceReqBuilder.getBodySizeLimit();

    Assert.assertEquals(
        1,
        PipeTransferSliceReqBuilder.getSliceCount(
            createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), bodySizeLimit),
            bodySizeLimit));
    Assert.assertEquals(
        2,
        PipeTransferSliceReqBuilder.getSliceCount(
            createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), bodySizeLimit + 1),
            bodySizeLimit));
    Assert.assertEquals(
        2,
        PipeTransferSliceReqBuilder.getSliceCount(
            createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), bodySizeLimit * 2),
            bodySizeLimit));
  }

  @Test
  public void testSliceOrderIdIncreases() {
    final int firstOrderId = PipeTransferSliceReqBuilder.nextSliceOrderId();

    Assert.assertEquals(firstOrderId + 1, PipeTransferSliceReqBuilder.nextSliceOrderId());
  }

  @Test
  public void testPipeTransferSliceReqFromLegacyV13Body() throws IOException {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_SLICE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(7, outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_TABLET_RAW.getType(), outputStream);
      ReadWriteIOUtils.write(6, outputStream);
      ReadWriteIOUtils.write(new Binary(new byte[] {2, 3, 4}), outputStream);
      ReadWriteIOUtils.write(1, outputStream);
      ReadWriteIOUtils.write(2, outputStream);
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    final PipeTransferSliceReq sliceReq = PipeTransferSliceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(7, sliceReq.getOrderId());
    Assert.assertEquals(PipeRequestType.TRANSFER_TABLET_RAW.getType(), sliceReq.getOriginReqType());
    Assert.assertEquals(6, sliceReq.getOriginBodySize());
    Assert.assertArrayEquals(new byte[] {2, 3, 4}, sliceReq.getSliceBody());
    Assert.assertEquals(1, sliceReq.getSliceIndex());
    Assert.assertEquals(2, sliceReq.getSliceCount());
  }

  private static void assertSlice(
      final PipeTransferSliceReq sliceReq,
      final int expectedOrderId,
      final short expectedOriginReqType,
      final int expectedOriginBodySize,
      final byte[] expectedSliceBody,
      final int expectedSliceIndex,
      final int expectedSliceCount) {
    Assert.assertEquals(IoTDBSinkRequestVersion.VERSION_1.getVersion(), sliceReq.version);
    Assert.assertEquals(PipeRequestType.TRANSFER_SLICE.getType(), sliceReq.type);
    Assert.assertEquals(expectedOrderId, sliceReq.getOrderId());
    Assert.assertEquals(expectedOriginReqType, sliceReq.getOriginReqType());
    Assert.assertEquals(expectedOriginBodySize, sliceReq.getOriginBodySize());
    Assert.assertArrayEquals(expectedSliceBody, sliceReq.getSliceBody());
    Assert.assertEquals(expectedSliceIndex, sliceReq.getSliceIndex());
    Assert.assertEquals(expectedSliceCount, sliceReq.getSliceCount());
  }

  private static TPipeTransferReq copyOf(final TPipeTransferReq req) {
    final TPipeTransferReq copy = new TPipeTransferReq();
    copy.version = req.version;
    copy.type = req.type;
    copy.body = req.body.duplicate();
    return copy;
  }

  private static TPipeTransferReq createReq(final byte version, final int bodySize) {
    final byte[] body = new byte[bodySize];
    for (int i = 0; i < body.length; ++i) {
      body[i] = (byte) i;
    }

    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = version;
    req.type = (short) 123;
    req.body = ByteBuffer.wrap(body);
    return req;
  }
}
