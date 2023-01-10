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

package org.apache.iotdb.db.mpp.plan.plan.node.write;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class InsertTabletNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    InsertTabletNode insertTabletNode = getInsertTabletNode();

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    insertTabletNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_TABLET.getNodeType(), byteBuffer.getShort());

    Assert.assertEquals(insertTabletNode, InsertTabletNode.deserialize(byteBuffer));

    insertTabletNode = getInsertTabletNodeWithSchema();
    byteBuffer = ByteBuffer.allocate(10000);
    insertTabletNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_TABLET.getNodeType(), byteBuffer.getShort());

    Assert.assertEquals(insertTabletNode, InsertTabletNode.deserialize(byteBuffer));
  }

  @Test
  public void testSerializeAndDeserializeForWAL() throws IllegalPathException, IOException {
    InsertTabletNode insertTabletNode = getInsertTabletNodeWithSchema();

    int serializedSize = insertTabletNode.serializedSize();

    byte[] bytes = new byte[serializedSize];
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));

    insertTabletNode.serializeToWAL(walBuffer);
    Assert.assertFalse(walBuffer.getBuffer().hasRemaining());

    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));

    Assert.assertEquals(PlanNodeType.INSERT_TABLET.getNodeType(), dataInputStream.readShort());

    InsertTabletNode tmpNode = InsertTabletNode.deserializeFromWAL(dataInputStream);
    tmpNode.setPlanNodeId(insertTabletNode.getPlanNodeId());

    tmpNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN)
        });
    Assert.assertEquals(insertTabletNode, tmpNode);
  }

  private InsertTabletNode getInsertTabletNode() throws IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    TSDataType[] dataTypes = new TSDataType[5];
    dataTypes[0] = TSDataType.DOUBLE;
    dataTypes[1] = TSDataType.FLOAT;
    dataTypes[2] = TSDataType.INT64;
    dataTypes[3] = TSDataType.INT32;
    dataTypes[4] = TSDataType.BOOLEAN;

    Object[] columns = new Object[5];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0;
      ((float[]) columns[1])[r] = 2;
      ((long[]) columns[2])[r] = 10000;
      ((int[]) columns[3])[r] = 100;
      ((boolean[]) columns[4])[r] = false;
    }

    InsertTabletNode tabletNode =
        new InsertTabletNode(
            new PlanNodeId("plannode 1"),
            new PartialPath("root.isp.d1"),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            dataTypes,
            times,
            null,
            columns,
            times.length);

    return tabletNode;
  }

  private InsertTabletNode getInsertTabletNodeWithSchema() throws IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    TSDataType[] dataTypes = new TSDataType[5];
    dataTypes[0] = TSDataType.DOUBLE;
    dataTypes[1] = TSDataType.FLOAT;
    dataTypes[2] = TSDataType.INT64;
    dataTypes[3] = TSDataType.INT32;
    dataTypes[4] = TSDataType.BOOLEAN;

    Object[] columns = new Object[5];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0;
      ((float[]) columns[1])[r] = 2;
      ((long[]) columns[2])[r] = 10000;
      ((int[]) columns[3])[r] = 100;
      ((boolean[]) columns[4])[r] = false;
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("plannode 1"),
            new PartialPath("root.isp.d1"),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN)
        });

    return insertTabletNode;
  }
}
