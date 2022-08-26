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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class InsertMultiTabletsNodeSerdeTest {

  @Test
  public void testInsertMultiTabletPlan() throws MetadataException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    Object[] columns = new Object[6];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];
    columns[5] = new Binary[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0;
      ((float[]) columns[1])[r] = 2;
      ((long[]) columns[2])[r] = 10000;
      ((int[]) columns[3])[r] = 100;
      ((boolean[]) columns[4])[r] = false;
      ((Binary[]) columns[5])[r] = new Binary("hh" + r);
    }

    PlanNodeId planNodeId = new PlanNodeId("plan node 1");

    InsertMultiTabletsNode insertMultiTabletsNode = new InsertMultiTabletsNode(planNodeId);

    for (int i = 0; i < 10; i++) {
      insertMultiTabletsNode.addInsertTabletNode(
          new InsertTabletNode(
              planNodeId,
              new PartialPath("root.multi.d" + i),
              false,
              new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
              dataTypes,
              times,
              null,
              columns,
              times.length),
          i);
    }

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    insertMultiTabletsNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_MULTI_TABLET.getNodeType(), byteBuffer.getShort());

    Assert.assertEquals(insertMultiTabletsNode, InsertMultiTabletsNode.deserialize(byteBuffer));
  }
}
