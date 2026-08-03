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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

// Covers Object-batch request serialization.
// Ensures a mixed base-tablet + Object-chunk payload survives toTPipeTransferReq /
// fromTPipeTransferReq with order, batch id, sequence id and last flag preserved.
public class PipeTransferTabletObjectBatchReqTest {

  // Round-trips one INT32 tablet followed by one OBJECT tablet and checks reconstructed
  // measurements stay in the original order.
  @Test
  public void testOrderedRoundTrip() throws Exception {
    final Tablet base =
        new Tablet(
            "table1", Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32)), 1);
    base.setColumnCategories(Collections.singletonList(ColumnCategory.FIELD));
    base.addTimestamp(0, 1L);
    base.addValue(0, 0, 1);
    base.setRowSize(1);

    final Tablet objectChunk =
        new Tablet(
            "table1", Collections.singletonList(new MeasurementSchema("o1", TSDataType.OBJECT)), 1);
    objectChunk.setColumnCategories(Collections.singletonList(ColumnCategory.FIELD));
    objectChunk.addTimestamp(0, 1L);
    objectChunk.addValue(0, 0, true, 0L, new byte[] {1, 2, 3});
    objectChunk.setRowSize(1);

    final PipeTransferTabletObjectBatchReq deserialized =
        PipeTransferTabletObjectBatchReq.fromTPipeTransferReq(
            PipeTransferTabletObjectBatchReq.toTPipeTransferReq(
                1L,
                0,
                true,
                Arrays.asList(base, objectChunk),
                Arrays.asList(false, false),
                "pipe_object_write"));

    Assert.assertEquals(2, deserialized.getTabletRequests().size());
    Assert.assertEquals(1L, deserialized.getBatchId());
    Assert.assertEquals(0, deserialized.getSequenceId());
    Assert.assertTrue(deserialized.isLast());
    Assert.assertEquals(
        "s1", deserialized.getTabletRequests().get(0).constructStatement().getMeasurements()[0]);
    Assert.assertEquals(
        "o1", deserialized.getTabletRequests().get(1).constructStatement().getMeasurements()[0]);
  }
}
