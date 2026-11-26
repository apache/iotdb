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
import java.util.Optional;

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

    final PipeTransferSliceReq firstSlice =
        PipeTransferSliceReqBuilder.buildSliceReq(req, 123, 0, 3, bodySizeLimit);
    final PipeTransferSliceReq secondSlice =
        PipeTransferSliceReqBuilder.buildSliceReq(req, 123, 1, 3, bodySizeLimit);
    final PipeTransferSliceReq thirdSlice =
        PipeTransferSliceReqBuilder.buildSliceReq(req, 123, 2, 3, bodySizeLimit);

    Assert.assertArrayEquals(new byte[] {0, 1, 2, 3}, firstSlice.getSliceBody());
    Assert.assertArrayEquals(new byte[] {4, 5, 6, 7}, secondSlice.getSliceBody());
    Assert.assertArrayEquals(new byte[] {8, 9}, thirdSlice.getSliceBody());
    Assert.assertEquals(0, firstSlice.getSliceIndex());
    Assert.assertEquals(1, secondSlice.getSliceIndex());
    Assert.assertEquals(2, thirdSlice.getSliceIndex());
    Assert.assertEquals(3, firstSlice.getSliceCount());
    Assert.assertEquals(req.getType(), firstSlice.getOriginReqType());
    Assert.assertEquals(10, firstSlice.getOriginBodySize());
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

  @Test
  public void testSliceReqHandlerRejectsOversizedSlices() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 6);
    final PipeTransferSliceReq firstSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 0, 2, req.body.duplicate(), 0, 4);
    final PipeTransferSliceReq oversizedSecondSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 1, 2, req.body.duplicate(), 2, 6);

    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();
    Assert.assertTrue(handler.receiveSlice(firstSlice));
    Assert.assertFalse(handler.receiveSlice(oversizedSecondSlice));
    Assert.assertFalse(handler.makeReqIfComplete().isPresent());
  }

  @Test
  public void testSliceReqHandlerAssemblesCompleteRequest() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 6);
    final PipeTransferSliceReq firstSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 0, 2, req.body.duplicate(), 0, 4);
    final PipeTransferSliceReq secondSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 1, 2, req.body.duplicate(), 4, 6);

    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();
    Assert.assertTrue(handler.receiveSlice(firstSlice));
    Assert.assertFalse(handler.makeReqIfComplete().isPresent());
    Assert.assertTrue(handler.receiveSlice(secondSlice));

    final Optional<TPipeTransferReq> completedReq = handler.makeReqIfComplete();
    Assert.assertTrue(completedReq.isPresent());

    final TPipeTransferReq assembledReq = completedReq.get();
    Assert.assertEquals(IoTDBSinkRequestVersion.VERSION_1.getVersion(), assembledReq.getVersion());
    Assert.assertEquals(req.getType(), assembledReq.getType());

    final byte[] assembledBody = new byte[assembledReq.body.remaining()];
    assembledReq.body.get(assembledBody);
    Assert.assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5}, assembledBody);
  }

  @Test
  public void testSliceReqHandlerRejectsInvalidMetadata() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 2);
    final PipeTransferSliceReq invalidSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 0, 0, req.body.duplicate(), 0, 1);

    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();

    Assert.assertFalse(handler.receiveSlice(invalidSlice));
    Assert.assertFalse(handler.makeReqIfComplete().isPresent());
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
