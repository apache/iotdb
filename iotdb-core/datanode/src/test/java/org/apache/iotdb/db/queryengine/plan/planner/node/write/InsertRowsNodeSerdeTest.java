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

package org.apache.iotdb.db.queryengine.plan.planner.node.write;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class InsertRowsNodeSerdeTest {

  @Test
  public void TestSerializeAndDeserialize() throws IllegalPathException {
    InsertRowsNode node = new InsertRowsNode(new PlanNodeId("plan node 1"));
    node.addOneInsertRowNode(
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            1000L,
            new Object[] {1.0, 2f, 300L},
            false),
        0);

    node.addOneInsertRowNode(
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d2"),
            false,
            new String[] {"s1", "s4"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.BOOLEAN},
            2000L,
            new Object[] {2.0, false},
            false),
        1);

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    node.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_ROWS.getNodeType(), byteBuffer.getShort());

    Assert.assertEquals(node, InsertRowsNode.deserialize(byteBuffer));
  }

  @Test
  public void testSerializeAndDeserializeForWAL() throws IllegalPathException, IOException {
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId("plan node 1"));
    insertRowsNode.addOneInsertRowNode(
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.DOUBLE),
              new MeasurementSchema("s2", TSDataType.FLOAT),
              new MeasurementSchema("s3", TSDataType.INT64)
            },
            1000L,
            new Object[] {1.0, 2f, 300L},
            false),
        0);

    insertRowsNode.addOneInsertRowNode(
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath("root.sg.d2"),
            false,
            new String[] {"s1", "s4"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.BOOLEAN},
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.DOUBLE),
              new MeasurementSchema("s4", TSDataType.BOOLEAN),
            },
            2000L,
            new Object[] {2.0, false},
            false),
        1);

    int serializedSize = insertRowsNode.serializedSize();

    byte[] bytes = new byte[serializedSize];
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));

    insertRowsNode.serializeToWAL(walBuffer);
    Assert.assertFalse(walBuffer.getBuffer().hasRemaining());

    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));

    Assert.assertEquals(PlanNodeType.INSERT_ROWS.getNodeType(), dataInputStream.readShort());

    InsertRowsNode tmpNode = InsertRowsNode.deserializeFromWAL(dataInputStream);
    tmpNode.setPlanNodeId(insertRowsNode.getPlanNodeId());
    Assert.assertEquals(insertRowsNode, tmpNode);
  }
}
