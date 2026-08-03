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

import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// Covers Object tablet batch accounting.
// Confirms Object batches still use the parent batch's in-memory tablet size / delay trigger for
// emit decisions, while Object file size is accounted separately and lazily.
public class PipeTabletObjectEventBatchTest {

  // Verifies that adding one tablet event records the tablet RAM footprint through the inherited
  // metric callback and still honors the zero-delay emit trigger.
  @Test
  public void testUseParentBatchMemoryAndTimeTrigger() throws Exception {
    final AtomicLong recordedBufferSize = new AtomicLong();
    final AtomicLong recordedEventCount = new AtomicLong();
    final PipeTabletObjectEventBatch batch =
        new PipeTabletObjectEventBatch(
            0,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            1,
            (timeInterval, bufferSize, eventCount) -> {
              recordedBufferSize.set(bufferSize);
              recordedEventCount.set(eventCount);
            });
    final PipeRawTabletInsertionEvent event = mock(PipeRawTabletInsertionEvent.class);
    final Tablet tablet = createTablet("raw");
    when(event.increaseReferenceCount(anyString())).thenReturn(true);
    when(event.convertToTablet()).thenReturn(tablet);

    Assert.assertTrue(batch.onEvent(event));
    Assert.assertEquals(tablet.ramBytesUsed(), recordedBufferSize.get());
    Assert.assertEquals(1, recordedEventCount.get());
  }

  private static Tablet createTablet(final String measurement) {
    final Tablet tablet =
        new Tablet(
            "table",
            Collections.singletonList(new MeasurementSchema(measurement, TSDataType.INT32)),
            1);
    tablet.setColumnCategories(Collections.singletonList(ColumnCategory.FIELD));
    tablet.addTimestamp(0, 1L);
    tablet.addValue(measurement, 0, 1);
    tablet.setRowSize(1);
    return tablet;
  }
}
