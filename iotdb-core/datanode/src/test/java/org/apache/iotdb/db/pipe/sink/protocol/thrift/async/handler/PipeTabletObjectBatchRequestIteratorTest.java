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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletObjectEventBatch;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Covers Object-batch request sequence resume.
// Validates that resetToSequence can restart the lazy request stream at any previously
// emitted sequence id and then reproduce the remaining requests bit-identically. This is the sender
// side of PIPE_TRANSFER_TABLET_OBJECT_BATCH_SEQUENCE_RESET handling.
public class PipeTabletObjectBatchRequestIteratorTest {

  private static final int OBJECT_COUNT = 100;
  private static final int OBJECT_SIZE_IN_BYTES = 1024 * 1024;
  private static final long MAX_REQUEST_SIZE_IN_BYTES = OBJECT_SIZE_IN_BYTES * 10L + 64 * 1024;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  // Builds a large Object tablet that splits into multiple bounded requests, then resets to every
  // sequence id and checks the remaining stream matches the original emission.
  @Test
  public void testResetToEveryObjectBatchSequence() throws Exception {
    final File tsFile = temporaryFolder.newFile("1-0-0-0.tsfile");
    final File objectDir = new File(tsFile.getParentFile(), "1-0-0-0");
    Assert.assertTrue(objectDir.mkdirs());

    final PipeRawTabletInsertionEvent event =
        new PipeRawTabletInsertionEvent(createObjectTabletAndFiles(objectDir), false);
    event.setTsFileResource(new TsFileResource(tsFile));
    // Simulate the source holder that owns a live event before it is added to a batch.
    Assert.assertTrue(event.increaseReferenceCount("test source"));
    final PipeTabletObjectEventBatch batch =
        new PipeTabletObjectEventBatch(
            Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, MAX_REQUEST_SIZE_IN_BYTES, null);
    try {
      Assert.assertFalse(batch.onEvent(event));
      Assert.assertEquals(1, batch.size());
      final PipeTabletObjectBatchRequestIterator requestIterator =
          new PipeTabletObjectBatchRequestIterator(batch.emit(1));
      final List<TPipeTransferReq> normalRequests = new ArrayList<>();
      while (requestIterator.hasNext()) {
        normalRequests.add(requestIterator.next());
      }
      Assert.assertTrue(normalRequests.size() > 1);

      for (int expectedSequenceId = 0;
          expectedSequenceId < normalRequests.size();
          expectedSequenceId++) {
        requestIterator.resetToSequence(expectedSequenceId);
        Assert.assertEquals(
            expectedSequenceId, getSequenceId(normalRequests.get(expectedSequenceId)));
        for (int sequenceId = expectedSequenceId;
            sequenceId < normalRequests.size();
            sequenceId++) {
          Assert.assertTrue(requestIterator.hasNext());
          final TPipeTransferReq retryRequest = requestIterator.next();
          Assert.assertEquals(sequenceId, getSequenceId(retryRequest));
          assertSameRequest(normalRequests.get(sequenceId), retryRequest);
        }
        Assert.assertFalse(requestIterator.hasNext());
      }
      requestIterator.close();
    } finally {
      batch.close();
      if (!event.isReleased()) {
        event.clearReferenceCount("test cleanup");
      }
    }
  }

  private Tablet createObjectTabletAndFiles(final File objectDir) throws Exception {
    final Tablet tablet =
        new Tablet(
            "root.test.d",
            Collections.singletonList(new MeasurementSchema("object", TSDataType.OBJECT)),
            OBJECT_COUNT);
    final Binary[] objectValues = (Binary[]) tablet.getValues()[0];
    tablet.initBitMaps();
    for (int index = 0; index < OBJECT_COUNT; index++) {
      final IObjectPath objectPath =
          IObjectPath.Factory.FACTORY.create(0, index, new StringArrayDeviceID("d"), "object");
      final File objectFile = new File(objectDir, objectPath.toString());
      Assert.assertTrue(objectFile.getParentFile().mkdirs() || objectFile.getParentFile().exists());
      try (final RandomAccessFile writer = new RandomAccessFile(objectFile, "rw")) {
        writer.setLength(OBJECT_SIZE_IN_BYTES);
      }

      objectValues[index] = ObjectTypeUtils.generateObjectBinary(OBJECT_SIZE_IN_BYTES, objectPath);
      tablet.getBitMaps()[0].unmark(index);
      tablet.addTimestamp(index, index);
    }
    tablet.setRowSize(OBJECT_COUNT);
    return tablet;
  }

  private static int getSequenceId(final TPipeTransferReq request) {
    return ByteBuffer.wrap(request.getBody()).getInt(Integer.BYTES + Long.BYTES);
  }

  private static void assertSameRequest(
      final TPipeTransferReq expectedRequest, final TPipeTransferReq actualRequest) {
    Assert.assertEquals(expectedRequest.getVersion(), actualRequest.getVersion());
    Assert.assertEquals(expectedRequest.getType(), actualRequest.getType());
    Assert.assertArrayEquals(expectedRequest.getBody(), actualRequest.getBody());
  }
}
