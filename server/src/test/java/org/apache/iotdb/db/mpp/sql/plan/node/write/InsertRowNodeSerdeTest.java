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
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class InsertRowNodeSerdeTest {

  @Test
  public void TestSerializeAndDeserialize() throws IllegalPathException {
    InsertRowNode insertRowNode = getInsertRowNode();

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    insertRowNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_ROW.ordinal(), byteBuffer.getShort());

    Assert.assertEquals(InsertRowNode.deserialize(byteBuffer), insertRowNode);

    // Test with failed column
    insertRowNode = getInsertRowNodeWithFailedColumn();
    byteBuffer = ByteBuffer.allocate(10000);
    insertRowNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_ROW.ordinal(), byteBuffer.getShort());

    InsertRowNode tmpNode = InsertRowNode.deserialize(byteBuffer);

    Assert.assertEquals(tmpNode.getTime(), insertRowNode.getTime());
    Assert.assertEquals(tmpNode.getMeasurements(), new String[] {"s1", "s3", "s5"});
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
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN)
        },
        dataTypes,
        time,
        columns);
  }

  private InsertRowNode getInsertRowNodeWithFailedColumn() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE, null, TSDataType.INT64, null, TSDataType.BOOLEAN,
        };

    Object[] columns = new Object[5];
    columns[0] = 1.0;
    columns[1] = null;
    columns[2] = 10000l;
    columns[3] = null;
    columns[4] = false;

    return new InsertRowNode(
        new PlanNodeId("plannode 1"),
        new PartialPath("root.isp.d1"),
        false,
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          null,
          new MeasurementSchema("s3", TSDataType.INT64),
          null,
          new MeasurementSchema("s5", TSDataType.BOOLEAN)
        },
        dataTypes,
        time,
        columns);
  }
}
