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

package org.apache.iotdb.confignode.manager.pipe.sink;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV1Req;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV2Req;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.confignode.persistence.schema.CNSnapshotFileType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipeConfigNodeThriftRequestTest {

  private static final String TIME_PRECISION = "ms";

  @Test
  public void testPipeTransferConfigHandshakeReq() throws IOException {
    PipeTransferConfigNodeHandshakeV1Req req =
        PipeTransferConfigNodeHandshakeV1Req.toTPipeTransferReq(TIME_PRECISION);
    PipeTransferConfigNodeHandshakeV1Req deserializeReq =
        PipeTransferConfigNodeHandshakeV1Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getTimestampPrecision(), deserializeReq.getTimestampPrecision());
  }

  @Test
  public void testPipeTransferConfigHandshakeReqFromLegacyV13Body() throws IOException {
    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.HANDSHAKE_CONFIGNODE_V1,
            serializeLegacyHandshakeV1Body(TIME_PRECISION));

    final PipeTransferConfigNodeHandshakeV1Req deserializeReq =
        PipeTransferConfigNodeHandshakeV1Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(TIME_PRECISION, deserializeReq.getTimestampPrecision());
  }

  @Test
  public void testPipeTransferConfigHandshakeV2Req() throws IOException {
    final Map<String, String> params = new HashMap<>();
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION, TIME_PRECISION);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, "cluster");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, "root");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, "root");

    final PipeTransferConfigNodeHandshakeV2Req req =
        PipeTransferConfigNodeHandshakeV2Req.toTPipeTransferReq(params);
    final PipeTransferConfigNodeHandshakeV2Req deserializeReq =
        PipeTransferConfigNodeHandshakeV2Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(params, deserializeReq.getParams());
  }

  @Test
  public void testPipeTransferConfigHandshakeV2ReqFromLegacyV13Body() throws IOException {
    final Map<String, String> params = new HashMap<>();
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION, TIME_PRECISION);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, "cluster");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, "root");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, "root");

    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.HANDSHAKE_CONFIGNODE_V2, serializeLegacyHandshakeV2Body(params));

    final PipeTransferConfigNodeHandshakeV2Req deserializeReq =
        PipeTransferConfigNodeHandshakeV2Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(params, deserializeReq.getParams());
  }

  @Test
  public void testPipeTransferConfigPlanReq() {
    PipeTransferConfigPlanReq req =
        PipeTransferConfigPlanReq.toTPipeTransferReq(new ActiveCQPlan("cqId", "md5"));
    PipeTransferConfigPlanReq deserializeReq = PipeTransferConfigPlanReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());
  }

  @Test
  public void testPipeTransferConfigPlanReqFromLegacyV13Body() {
    final ActiveCQPlan plan = new ActiveCQPlan("cqId", "md5");
    final ByteBuffer legacyBody = plan.serializeToByteBuffer();
    final TPipeTransferReq req =
        legacyTransferReq(PipeRequestType.TRANSFER_CONFIG_PLAN, legacyBody);

    final PipeTransferConfigPlanReq deserializeReq =
        PipeTransferConfigPlanReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(byteBufferToByteArray(legacyBody), deserializeReq.getBody());
  }

  @Test
  public void testPipeTransferConfigSnapshotPieceReq() throws IOException {
    byte[] body = "testPipeTransferConfigSnapshotPieceReq".getBytes();
    String fileName = "1.temp";

    PipeTransferConfigSnapshotPieceReq req =
        PipeTransferConfigSnapshotPieceReq.toTPipeTransferReq(fileName, 0, body);
    PipeTransferConfigSnapshotPieceReq deserializeReq =
        PipeTransferConfigSnapshotPieceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferConfigSnapshotPieceReqFromLegacyV13Body() throws IOException {
    final byte[] body = "legacyConfigSnapshotPiece".getBytes();
    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_PIECE,
            serializeLegacyFilePieceBody("cluster_schema.bin", 10L, body));

    final PipeTransferConfigSnapshotPieceReq deserializeReq =
        PipeTransferConfigSnapshotPieceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals("cluster_schema.bin", deserializeReq.getFileName());
    Assert.assertEquals(10L, deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(body, deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferConfigSnapshotSealReq() throws IOException {
    String snapshotName = "cluster_schema.bin";
    String templateInfoName = "template_info.bin";
    CNSnapshotFileType fileType = CNSnapshotFileType.SCHEMA;
    // CreateDatabase
    String typeString = "200";

    PipeTransferConfigSnapshotSealReq req =
        PipeTransferConfigSnapshotSealReq.toTPipeTransferReq(
            "root.**",
            "db",
            "table",
            true,
            true,
            snapshotName,
            100,
            templateInfoName,
            10,
            fileType,
            typeString,
            "");
    PipeTransferConfigSnapshotSealReq deserializeReq =
        PipeTransferConfigSnapshotSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getFileNames(), deserializeReq.getFileNames());
    Assert.assertEquals(req.getFileLengths(), deserializeReq.getFileLengths());
    Assert.assertEquals(req.getParameters(), deserializeReq.getParameters());
  }

  @Test
  public void testPipeTransferConfigSnapshotSealReqFromLegacyV13Body() throws IOException {
    final String snapshotName = "cluster_schema.bin";
    final String templateInfoName = "template_info.bin";
    final Map<String, String> parameters = new HashMap<>();
    parameters.put(ColumnHeaderConstant.PATH_PATTERN, "root.**");
    parameters.put(
        PipeTransferConfigSnapshotSealReq.FILE_TYPE,
        Byte.toString(CNSnapshotFileType.SCHEMA.getType()));
    parameters.put(ColumnHeaderConstant.TYPE, "200");

    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_SEAL,
            serializeLegacyFileSealV2Body(
                Arrays.asList(snapshotName, templateInfoName),
                Arrays.asList(100L, 10L),
                parameters));
    final PipeTransferConfigSnapshotSealReq deserializeReq =
        PipeTransferConfigSnapshotSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(
        Arrays.asList(snapshotName, templateInfoName), deserializeReq.getFileNames());
    Assert.assertEquals(Arrays.asList(100L, 10L), deserializeReq.getFileLengths());
    Assert.assertEquals(parameters, deserializeReq.getParameters());
    Assert.assertTrue(
        PipeTransferFileSealReqV2.isTreeModelDataAllowedToBeCaptured(
            deserializeReq.getParameters()));
    Assert.assertFalse(
        PipeTransferFileSealReqV2.isTableModelDataAllowedToBeCaptured(
            deserializeReq.getParameters()));
  }

  private static TPipeTransferReq legacyTransferReq(
      final PipeRequestType requestType, final ByteBuffer body) {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = requestType.getType();
    req.body = body;
    return req;
  }

  private static byte[] byteBufferToByteArray(final ByteBuffer byteBuffer) {
    final ByteBuffer duplicatedBuffer = byteBuffer.duplicate();
    final byte[] bytes = new byte[duplicatedBuffer.remaining()];
    duplicatedBuffer.get(bytes);
    return bytes;
  }

  private static ByteBuffer serializeLegacyFileSealV2Body(
      final List<String> fileNames,
      final List<Long> fileLengths,
      final Map<String, String> parameters)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileNames.size(), outputStream);
      for (final String fileName : fileNames) {
        ReadWriteIOUtils.write(fileName, outputStream);
      }
      ReadWriteIOUtils.write(fileLengths.size(), outputStream);
      for (final Long fileLength : fileLengths) {
        ReadWriteIOUtils.write(fileLength, outputStream);
      }
      ReadWriteIOUtils.write(parameters.size(), outputStream);
      for (final Map.Entry<String, String> entry : parameters.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static ByteBuffer serializeLegacyHandshakeV1Body(final String timestampPrecision)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(timestampPrecision, outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static ByteBuffer serializeLegacyHandshakeV2Body(final Map<String, String> params)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(params.size(), outputStream);
      for (final Map.Entry<String, String> entry : params.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static ByteBuffer serializeLegacyFilePieceBody(
      final String fileName, final long startWritingOffset, final byte[] filePiece)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(startWritingOffset, outputStream);
      ReadWriteIOUtils.write(new Binary(filePiece), outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }
}
