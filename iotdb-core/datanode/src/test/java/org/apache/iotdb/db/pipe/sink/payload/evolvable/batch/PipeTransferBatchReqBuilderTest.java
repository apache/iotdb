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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.batch;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY;

public class PipeTransferBatchReqBuilderTest {

  @Test
  public void testDetachedBatchCannotClearSubsequentEvent() throws Exception {
    final PipeTransferBatchReqBuilder builder =
        new PipeTransferBatchReqBuilder(
            new PipeParameters(Collections.singletonMap(CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY, "0")));
    final PipeRawTabletInsertionEvent firstEvent = createEvent(1);
    final PipeRawTabletInsertionEvent secondEvent = createEvent(2);

    try {
      builder.onEvent(firstEvent);
      final List<Pair<TEndPoint, PipeTabletEventBatch>> detachedBatches =
          builder.getAllNonEmptyAndShouldEmitBatchesAndDetach();
      Assert.assertEquals(1, detachedBatches.size());

      builder.onEvent(secondEvent);
      detachedBatches.get(0).getRight().closeAfterEventTransfer();

      Assert.assertEquals(1, builder.size());
      Assert.assertEquals(1, secondEvent.getReferenceCount());
      Assert.assertFalse(secondEvent.isReleased());

      // Simulate completion by the handler that owns the detached event reference.
      firstEvent.decreaseReferenceCount(getClass().getName(), false);
      Assert.assertTrue(firstEvent.isReleased());
    } finally {
      builder.close();
    }

    Assert.assertTrue(secondEvent.isReleased());
  }

  @Test
  public void testMemoryPressureKeepsExistingBatchEmittable() throws Exception {
    final PipeTabletEventBatch batch =
        new PipeTabletEventBatch(Integer.MAX_VALUE, Long.MAX_VALUE, null) {
          private int constructCount;

          @Override
          protected boolean constructBatch(final TabletInsertionEvent event) {
            increaseTotalBufferSizeAndUpdateMemoryBlock(
                constructCount++ == 0 ? 1 : Long.MAX_VALUE / 2);
            return true;
          }
        };

    try {
      Assert.assertFalse(batch.onEvent(createEvent(1)));
      Assert.assertThrows(
          PipeRuntimeOutOfMemoryCriticalException.class, () -> batch.onEvent(createEvent(2)));
      Assert.assertTrue(batch.shouldEmit());
    } finally {
      batch.close();
    }
  }

  private static PipeRawTabletInsertionEvent createEvent(final int value) {
    final Tablet tablet =
        new Tablet(
            IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.device"),
            Collections.singletonList("s1"),
            Collections.singletonList(TSDataType.INT32),
            1);
    tablet.addTimestamp(0, value);
    tablet.addValue("s1", 0, value);
    tablet.setRowSize(1);
    return new PipeRawTabletInsertionEvent(
        false, "root.test", null, "root.test", tablet, false, null, 0, null, null, false);
  }
}
