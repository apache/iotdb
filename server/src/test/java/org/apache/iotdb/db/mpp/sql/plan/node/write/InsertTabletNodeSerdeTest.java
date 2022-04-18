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

package org.apache.iotdb.db.mpp.sql.plan.node.write;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class InsertTabletNodeSerdeTest {

  @Test
  public void TestSerializeAndDeserialize() throws IllegalPathException {
    InsertTabletNode insertTabletNode = getInsertTabletNode();

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    insertTabletNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_TABLET.ordinal(), byteBuffer.getShort());

    Assert.assertEquals(InsertTabletNode.deserialize(byteBuffer), insertTabletNode);
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
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.DOUBLE),
              new MeasurementSchema("s2", TSDataType.FLOAT),
              new MeasurementSchema("s3", TSDataType.INT64),
              new MeasurementSchema("s4", TSDataType.INT32),
              new MeasurementSchema("s5", TSDataType.BOOLEAN)
            },
            dataTypes,
            times,
            null,
            columns,
            times.length);

    return tabletNode;
  }
}
