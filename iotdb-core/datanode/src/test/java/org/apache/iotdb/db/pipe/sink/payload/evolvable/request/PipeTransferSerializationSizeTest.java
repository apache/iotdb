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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

public class PipeTransferSerializationSizeTest {

  @Test
  public void testTabletRequestLengths() throws Exception {
    final Tablet tablet = createTablet();
    final String database = "pipe_db";
    Assert.assertEquals(
        PipeTransferTabletRawReq.calculateSerializedSize(tablet),
        PipeTransferTabletRawReq.toTPipeTransferReq(tablet, false).getBody().length);
    Assert.assertEquals(
        PipeTransferTabletRawReqV2.calculateSerializedSize(tablet, database),
        PipeTransferTabletRawReqV2.toTPipeTransferReq(tablet, false, database).getBody().length);
    Assert.assertEquals(
        PipeTransferTabletRawReq.calculateAirGapSerializedSize(tablet),
        PipeTransferTabletRawReq.toTPipeTransferBytes(tablet, false).length);
  }

  @Test
  public void testBinaryRequestLengths() throws Exception {
    final ByteBuffer payload = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
    final String database = "pipe_db";
    Assert.assertEquals(
        PipeTransferTabletBinaryReqV2.calculateSerializedSize(payload, database),
        PipeTransferTabletBinaryReqV2.toTPipeTransferReq(payload, database).getBody().length);
    Assert.assertEquals(
        PipeTransferTabletBinaryReqV2.calculateAirGapSerializedSize(payload, database),
        PipeTransferTabletBinaryReqV2.toTPipeTransferBytes(payload, database).length);
    Assert.assertEquals(
        PipeTransferTabletBinaryReq.calculateSerializedSize(payload),
        PipeTransferTabletBinaryReq.toTPipeTransferBytes(payload).length);
  }

  @Test
  public void testBatchRequestLengths() throws Exception {
    final ByteBuffer insertNode = ByteBuffer.wrap(new byte[] {1, 2});
    final ByteBuffer tablet = ByteBuffer.wrap(new byte[] {3, 4, 5});
    Assert.assertEquals(
        PipeTransferTabletBatchReq.calculateSerializedSize(
            Collections.singletonList(insertNode), Collections.singletonList(tablet)),
        PipeTransferTabletBatchReq.toTPipeTransferReq(
                Collections.singletonList(insertNode), Collections.singletonList(tablet))
            .getBody()
            .length);

    final String database = "db";
    Assert.assertEquals(
        PipeTransferTabletBatchReqV2.calculateSerializedSize(
            Collections.singletonList(insertNode),
            Collections.singletonList(tablet),
            Collections.singletonList(database),
            Collections.singletonList(database)),
        PipeTransferTabletBatchReqV2.toTPipeTransferReq(
                Collections.singletonList(insertNode),
                Collections.singletonList(tablet),
                Collections.singletonList(database),
                Collections.singletonList(database))
            .getBody()
            .length);
  }

  private static Tablet createTablet() {
    final Tablet tablet =
        new Tablet(
            "table1", Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32)), 1);
    tablet.setColumnCategories(Collections.singletonList(ColumnCategory.FIELD));
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, 1);
    tablet.setRowSize(1);
    return tablet;
  }
}
