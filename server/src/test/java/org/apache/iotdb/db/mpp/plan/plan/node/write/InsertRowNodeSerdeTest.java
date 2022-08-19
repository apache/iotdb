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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class InsertRowNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    InsertRowNode insertRowNode = getInsertRowNode();

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    insertRowNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), byteBuffer.getShort());

    Assert.assertEquals(insertRowNode, InsertRowNode.deserialize(byteBuffer));

    insertRowNode = getInsertRowNodeWithMeasurementSchemas();
    byteBuffer = ByteBuffer.allocate(10000);
    insertRowNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), byteBuffer.getShort());

    Assert.assertEquals(insertRowNode, InsertRowNode.deserialize(byteBuffer));

    insertRowNode = getInsertRowNodeWithStringValue();
    byteBuffer = ByteBuffer.allocate(10000);
    insertRowNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), byteBuffer.getShort());

    Assert.assertEquals(insertRowNode, InsertRowNode.deserialize(byteBuffer));
  }

  @Test
  public void TestSerializeAndDeserializeForWAL() throws IllegalPathException, IOException {
    InsertRowNode insertRowNode = getInsertRowNodeWithMeasurementSchemas();

    int serializedSize = insertRowNode.serializedSize();

    byte[] bytes = new byte[serializedSize];
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));

    insertRowNode.serializeToWAL(walBuffer);
    Assert.assertFalse(walBuffer.getBuffer().hasRemaining());

    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));

    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), dataInputStream.readShort());

    InsertRowNode tmpNode = InsertRowNode.deserializeFromWAL(dataInputStream);
    tmpNode.setPlanNodeId(insertRowNode.getPlanNodeId());
    tmpNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN)
        });
    Assert.assertEquals(insertRowNode, tmpNode);
  }

  private InsertRowNode getInsertRowNode() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
        };

    Object[] columns = new Object[5];
    columns[0] = 1.0;
    columns[1] = 2.0f;
    columns[2] = 10000l;
    columns[3] = 100;
    columns[4] = false;

    return new InsertRowNode(
        new PlanNodeId("plannode 1"),
        new PartialPath("root.isp.d1"),
        false,
        new String[] {"s1", "s2", "s3", "s4", "s5"},
        dataTypes,
        time,
        columns,
        false);
  }

  private InsertRowNode getInsertRowNodeWithMeasurementSchemas() throws IllegalPathException {
    long time = 80L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
        };

    Object[] columns = new Object[5];
    columns[0] = 5.0;
    columns[1] = 6.0f;
    columns[2] = 1000l;
    columns[3] = 10;
    columns[4] = true;

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("plannode 2"),
            new PartialPath("root.isp.d2"),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            dataTypes,
            time,
            columns,
            false);

    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN)
        });

    return insertRowNode;
  }

  private InsertRowNode getInsertRowNodeWithStringValue() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
        };

    Object[] columns = new Object[5];
    columns[0] = "1.0";
    columns[1] = "2.0";
    columns[2] = "10000";
    columns[3] = "100";
    columns[4] = "false";

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("plannode 1"),
            new PartialPath("root.isp.d1"),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            new TSDataType[5],
            time,
            columns,
            false);
    insertRowNode.setNeedInferType(true);
    return insertRowNode;
  }
}
