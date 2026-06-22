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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.request;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.pipe.sink.payload.thrift.PipeTransferReqTestUtils.assertVersionAndType;
import static org.apache.iotdb.commons.pipe.sink.payload.thrift.PipeTransferReqTestUtils.copyOf;

public class PipeTransferFileSealReqV2Test {

  @Test
  public void testSnapshotSealModelCaptureRules() {
    assertCapture("legacy v1.3", Collections.emptyMap(), true, false);
    assertCapture(
        "non-model parameter only",
        markerParameters(PipeTransferFileSealReqV2.DATABASE_PATTERN),
        true,
        false);
    assertCapture("tree only", markerParameters(PipeTransferFileSealReqV2.TREE), true, false);
    assertCapture("table only", markerParameters(PipeTransferFileSealReqV2.TABLE), false, true);
    assertCapture(
        "tree and table",
        markerParameters(PipeTransferFileSealReqV2.TREE, PipeTransferFileSealReqV2.TABLE),
        true,
        true);
  }

  @Test
  public void testSnapshotSealReqV2RoundTripKeepsFilesAndParameters() throws IOException {
    final List<String> fileNames = Arrays.asList("schema.snapshot", "template.snapshot");
    final List<Long> fileLengths = Arrays.asList(12L, 34L);
    final Map<String, String> parameters = snapshotParameters();

    final DummyFileSealReqV2 req =
        DummyFileSealReqV2.toTPipeTransferReq(fileNames, fileLengths, parameters);

    assertVersionAndType(
        req, IoTDBSinkRequestVersion.VERSION_1, PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL);
    Assert.assertEquals(fileNames, req.getFileNames());
    Assert.assertEquals(fileLengths, req.getFileLengths());
    Assert.assertEquals(parameters, req.getParameters());
    assertSnapshotSealBody(req.body.duplicate(), fileNames, fileLengths, parameters);

    final DummyFileSealReqV2 deserializedReq =
        (DummyFileSealReqV2) new DummyFileSealReqV2().translateFromTPipeTransferReq(copyOf(req));

    Assert.assertEquals(req.version, deserializedReq.version);
    Assert.assertEquals(req.type, deserializedReq.type);
    Assert.assertEquals(fileNames, deserializedReq.getFileNames());
    Assert.assertEquals(fileLengths, deserializedReq.getFileLengths());
    Assert.assertEquals(parameters, deserializedReq.getParameters());
  }

  @Test
  public void testSnapshotSealAirGapBytesKeepSameBodyFormat() throws IOException {
    final List<String> fileNames = Arrays.asList("schema.snapshot", "template.snapshot");
    final List<Long> fileLengths = Arrays.asList(12L, 34L);
    final Map<String, String> parameters = snapshotParameters();

    final ByteBuffer buffer =
        ByteBuffer.wrap(
            new DummyFileSealReqV2()
                .convertToTPipeTransferSnapshotSealBytes(fileNames, fileLengths, parameters));

    Assert.assertEquals(
        IoTDBSinkRequestVersion.VERSION_1.getVersion(), ReadWriteIOUtils.readByte(buffer));
    Assert.assertEquals(
        PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL.getType(),
        ReadWriteIOUtils.readShort(buffer));
    assertSnapshotSealBody(buffer, fileNames, fileLengths, parameters);
  }

  private static void assertCapture(
      final String caseName,
      final Map<String, String> parameters,
      final boolean expectedTreeCaptured,
      final boolean expectedTableCaptured) {
    Assert.assertEquals(
        caseName,
        expectedTreeCaptured,
        PipeTransferFileSealReqV2.isTreeModelDataAllowedToBeCaptured(parameters));
    Assert.assertEquals(
        caseName,
        expectedTableCaptured,
        PipeTransferFileSealReqV2.isTableModelDataAllowedToBeCaptured(parameters));
  }

  private static void assertSnapshotSealBody(
      final ByteBuffer body,
      final List<String> expectedFileNames,
      final List<Long> expectedFileLengths,
      final Map<String, String> expectedParameters) {
    final int fileNameSize = ReadWriteIOUtils.readInt(body);
    Assert.assertEquals(expectedFileNames.size(), fileNameSize);
    for (final String expectedFileName : expectedFileNames) {
      Assert.assertEquals(expectedFileName, ReadWriteIOUtils.readString(body));
    }

    final int fileLengthSize = ReadWriteIOUtils.readInt(body);
    Assert.assertEquals(expectedFileLengths.size(), fileLengthSize);
    for (final Long expectedFileLength : expectedFileLengths) {
      Assert.assertEquals(expectedFileLength.longValue(), ReadWriteIOUtils.readLong(body));
    }

    final int parameterSize = ReadWriteIOUtils.readInt(body);
    final Map<String, String> parameters = new LinkedHashMap<>();
    for (int i = 0; i < parameterSize; i++) {
      parameters.put(ReadWriteIOUtils.readString(body), ReadWriteIOUtils.readString(body));
    }
    Assert.assertEquals(expectedParameters, parameters);
    Assert.assertFalse(body.hasRemaining());
  }

  private static Map<String, String> markerParameters(final String... keys) {
    final Map<String, String> parameters = new LinkedHashMap<>();
    for (final String key : keys) {
      parameters.put(key, "");
    }
    return parameters;
  }

  private static Map<String, String> snapshotParameters() {
    final Map<String, String> parameters =
        markerParameters(PipeTransferFileSealReqV2.TREE, PipeTransferFileSealReqV2.TABLE);
    parameters.put(PipeTransferFileSealReqV2.DATABASE_PATTERN, "root.sg.*");
    return parameters;
  }

  private static class DummyFileSealReqV2 extends PipeTransferFileSealReqV2 {

    private static DummyFileSealReqV2 toTPipeTransferReq(
        final List<String> fileNames,
        final List<Long> fileLengths,
        final Map<String, String> parameters)
        throws IOException {
      return (DummyFileSealReqV2)
          new DummyFileSealReqV2().convertToTPipeTransferReq(fileNames, fileLengths, parameters);
    }

    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL;
    }
  }
}
