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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

  @Test
  public void testSerializeAndDeserializeRelational() throws IllegalPathException {
    for (String tableName : new String[] {"table1", "ta`ble1", "root.table1"}) {
      RelationalInsertTabletNode insertTabletNode = getRelationalInsertTabletNode(tableName);

      ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
      insertTabletNode.serialize(byteBuffer);
      byteBuffer.flip();

      Assert.assertEquals(
          PlanNodeType.RELATIONAL_INSERT_TABLET.getNodeType(), byteBuffer.getShort());

      Assert.assertEquals(insertTabletNode, RelationalInsertTabletNode.deserialize(byteBuffer));

      insertTabletNode = getRelationalInsertTabletNodeWithSchema(tableName);
      byteBuffer = ByteBuffer.allocate(10000);
      insertTabletNode.serialize(byteBuffer);
      byteBuffer.flip();

      Assert.assertEquals(
          PlanNodeType.RELATIONAL_INSERT_TABLET.getNodeType(), byteBuffer.getShort());

      Assert.assertEquals(insertTabletNode, RelationalInsertTabletNode.deserialize(byteBuffer));
    }
  }

  @Test
  public void testSerializeAndDeserializeForWALRelational() throws IOException {
    for (String tableName : new String[] {"table1", "ta`ble1", "root.table1"}) {
      RelationalInsertTabletNode insertTabletNode =
          getRelationalInsertTabletNodeWithSchema(tableName);

      int serializedSize = insertTabletNode.serializedSize();

      byte[] bytes = new byte[serializedSize];
      WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));

      insertTabletNode.serializeToWAL(walBuffer);
      Assert.assertFalse(walBuffer.getBuffer().hasRemaining());

      DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));

      Assert.assertEquals(
          PlanNodeType.RELATIONAL_INSERT_TABLET.getNodeType(), dataInputStream.readShort());

      RelationalInsertTabletNode tmpNode =
          RelationalInsertTabletNode.deserializeFromWAL(dataInputStream);
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
  }

  @Test
  public void testInitTabletValuesWithAllTypes()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    InsertTabletNode insertTabletNode = new InsertTabletNode(new PlanNodeId("1"));
    Method initTabletValuesMethod =
        InsertTabletNode.class.getDeclaredMethod(
            "initTabletValues", int.class, int.class, TSDataType[].class);
    initTabletValuesMethod.setAccessible(true);

    TSDataType[] dataTypes = {
      TSDataType.TEXT, TSDataType.FLOAT, TSDataType.INT32,
      TSDataType.TIMESTAMP, TSDataType.DOUBLE, TSDataType.BOOLEAN
    };

    int columnSize = dataTypes.length;
    int rowSize = 5;

    Object[] values =
        (Object[]) initTabletValuesMethod.invoke(insertTabletNode, columnSize, rowSize, dataTypes);

    // Assert the result
    Assert.assertEquals(columnSize, values.length);

    // Validate each element in the values array
    Assert.assertEquals(Binary[].class, values[0].getClass());
    Assert.assertEquals(float[].class, values[1].getClass());
    Assert.assertEquals(int[].class, values[2].getClass());
    Assert.assertEquals(long[].class, values[3].getClass());
    Assert.assertEquals(double[].class, values[4].getClass());
    Assert.assertEquals(boolean[].class, values[5].getClass());
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

  private RelationalInsertTabletNode getRelationalInsertTabletNode(String tableName)
      throws IllegalPathException {
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

    RelationalInsertTabletNode tabletNode =
        new RelationalInsertTabletNode(
            new PlanNodeId("plannode 1"),
            new PartialPath(tableName, false),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            dataTypes,
            times,
            null,
            columns,
            times.length,
            new TsTableColumnCategory[] {
              TsTableColumnCategory.ID,
              TsTableColumnCategory.MEASUREMENT,
              TsTableColumnCategory.ATTRIBUTE,
              TsTableColumnCategory.ID,
              TsTableColumnCategory.MEASUREMENT
            });

    return tabletNode;
  }

  private RelationalInsertTabletNode getRelationalInsertTabletNodeWithSchema(String tableName) {
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

    RelationalInsertTabletNode insertTabletNode =
        new RelationalInsertTabletNode(
            new PlanNodeId("plannode 1"),
            new PartialPath(tableName, false),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            dataTypes,
            times,
            null,
            columns,
            times.length,
            new TsTableColumnCategory[] {
              TsTableColumnCategory.ID,
              TsTableColumnCategory.MEASUREMENT,
              TsTableColumnCategory.ATTRIBUTE,
              TsTableColumnCategory.ID,
              TsTableColumnCategory.MEASUREMENT
            });
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
