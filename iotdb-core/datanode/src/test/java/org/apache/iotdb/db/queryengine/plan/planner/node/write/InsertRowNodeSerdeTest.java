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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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
    insertRowNode.setLastFragment(true);

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
          new MeasurementSchema("\u6e29\u5ea6", TSDataType.DOUBLE),
          new MeasurementSchema("\u6e7f\u5ea6", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN)
        });
    Assert.assertEquals(insertRowNode, tmpNode);
    Assert.assertTrue(tmpNode.isLastFragment());
  }

  @Test
  public void testDeserializeLegacyWAL() throws IllegalPathException, IOException {
    InsertRowNode insertRowNode = getInsertRowNodeWithMeasurementSchemas();
    insertRowNode.setSearchIndex(123L);

    byte[] bytes = new byte[insertRowNode.serializedSize()];
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));
    insertRowNode.serializeToWAL(walBuffer);

    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), byteBuffer.getShort());
    Assert.assertEquals(123L, byteBuffer.getLong());

    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
    dataInputStream.readShort();

    InsertRowNode tmpNode = InsertRowNode.deserializeFromWAL(dataInputStream);
    tmpNode.setPlanNodeId(insertRowNode.getPlanNodeId());
    tmpNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("\u6e29\u5ea6", TSDataType.DOUBLE),
          new MeasurementSchema("\u6e7f\u5ea6", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN)
        });
    Assert.assertEquals(insertRowNode, tmpNode);
    Assert.assertEquals(123L, tmpNode.getSearchIndex());
    Assert.assertFalse(tmpNode.isLastFragment());
  }

  @Test
  public void testDeserializeLegacyWALRelational() throws IOException {
    RelationalInsertRowNode insertRowNode = getRelationalInsertRowNodeWithMeasurementSchemas();
    insertRowNode.setSearchIndex(123L);

    byte[] bytes = new byte[insertRowNode.serializedSize()];
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));
    insertRowNode.serializeToWAL(walBuffer);

    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    Assert.assertEquals(PlanNodeType.RELATIONAL_INSERT_ROW.getNodeType(), byteBuffer.getShort());
    Assert.assertEquals(123L, byteBuffer.getLong());

    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
    dataInputStream.readShort();

    RelationalInsertRowNode tmpNode = RelationalInsertRowNode.deserializeFromWAL(dataInputStream);
    tmpNode.setPlanNodeId(insertRowNode.getPlanNodeId());
    tmpNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("id", TSDataType.STRING),
          new MeasurementSchema("attr", TSDataType.TEXT),
          new MeasurementSchema("value", TSDataType.INT64)
        });
    Assert.assertEquals(insertRowNode, tmpNode);
    Assert.assertEquals(123L, tmpNode.getSearchIndex());
    Assert.assertFalse(tmpNode.isLastFragment());
  }

  @Test
  public void testRelationalSerializedSizeWithFailedMeasurement() {
    RelationalInsertRowNode insertRowNode = getRelationalInsertRowNodeWithMeasurementSchemas();
    insertRowNode.markFailedMeasurement(1);
    insertRowNode.setFailedMeasurementNumber(1);

    ByteBuffer byteBuffer = ByteBuffer.allocate(insertRowNode.serializedSize());
    insertRowNode.serializeToWAL(new WALByteBufferForTest(byteBuffer));

    Assert.assertEquals(insertRowNode.serializedSize(), byteBuffer.position());
  }

  @Test
  public void testSerializeSkipsRetainedMeasurementWithMissingValue() throws IllegalPathException {
    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("plannode missing value"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT32, TSDataType.INT32},
            1L,
            new Object[] {1, 2},
            false);

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    insertRowNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), byteBuffer.getShort());

    InsertRowNode tmpNode = InsertRowNode.deserialize(byteBuffer);
    Assert.assertArrayEquals(new String[] {"s1", "s2"}, tmpNode.getMeasurements());
  }

  @Test
  public void testSerializeKeepsNullRowValueWithoutType() throws IllegalPathException {
    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("plannode null value"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, null},
            1L,
            new Object[] {1, null},
            false);

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    insertRowNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), byteBuffer.getShort());

    InsertRowNode tmpNode = InsertRowNode.deserialize(byteBuffer);
    Assert.assertArrayEquals(new String[] {"s1", "s2"}, tmpNode.getMeasurements());
    Assert.assertArrayEquals(new TSDataType[] {TSDataType.INT32, null}, tmpNode.getDataTypes());
    Assert.assertArrayEquals(new Object[] {1, null}, tmpNode.getValues());
  }

  @Test
  public void testDeserializeFromWALSkipsRetainedMeasurementWithNullSchema()
      throws IllegalPathException, IOException {
    InsertRowNode insertRowNode = getInsertRowNodeWithMeasurementSchemas();
    insertRowNode.getMeasurementSchemas()[1] = null;

    byte[] bytes = new byte[insertRowNode.serializedSize()];
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));
    insertRowNode.serializeToWAL(walBuffer);
    Assert.assertFalse(walBuffer.getBuffer().hasRemaining());

    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), dataInputStream.readShort());

    InsertRowNode tmpNode = InsertRowNode.deserializeFromWAL(dataInputStream);
    Assert.assertArrayEquals(
        new String[] {"\u6e29\u5ea6", "s3", "s4", "s5"}, tmpNode.getMeasurements());
  }

  @Test
  public void testDeserializeFromWALSkipsRetainedMeasurementWithMissingValue()
      throws IllegalPathException, IOException {
    InsertRowNode insertRowNode = getInsertRowNodeWithMeasurementSchemas();
    insertRowNode.setValues(new Object[] {5.0, 6.0f});

    byte[] bytes = new byte[insertRowNode.serializedSize()];
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));
    insertRowNode.serializeToWAL(walBuffer);
    Assert.assertFalse(walBuffer.getBuffer().hasRemaining());

    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), dataInputStream.readShort());

    InsertRowNode tmpNode = InsertRowNode.deserializeFromWAL(dataInputStream);
    Assert.assertArrayEquals(
        new String[] {"\u6e29\u5ea6", "\u6e7f\u5ea6"}, tmpNode.getMeasurements());
  }

  @Test
  public void testRelationalDeserializeFromWALSkipsRetainedMeasurementWithNullCategory()
      throws IOException {
    RelationalInsertRowNode insertRowNode = getRelationalInsertRowNodeWithMeasurementSchemas();
    insertRowNode.getColumnCategories()[1] = null;

    byte[] bytes = new byte[insertRowNode.serializedSize()];
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));
    insertRowNode.serializeToWAL(walBuffer);
    Assert.assertFalse(walBuffer.getBuffer().hasRemaining());

    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
    Assert.assertEquals(
        PlanNodeType.RELATIONAL_INSERT_ROW.getNodeType(), dataInputStream.readShort());

    RelationalInsertRowNode tmpNode = RelationalInsertRowNode.deserializeFromWAL(dataInputStream);
    Assert.assertArrayEquals(new String[] {"id", "value"}, tmpNode.getMeasurements());
    Assert.assertArrayEquals(
        new TsTableColumnCategory[] {TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD},
        tmpNode.getColumnCategories());
  }

  @Test
  public void testDeserializeFromWALWithMarkedFailedMeasurementOnly()
      throws IllegalPathException, IOException {
    InsertRowNode insertRowNode = getInsertRowNodeWithMeasurementSchemas();
    insertRowNode.markFailedMeasurement(1);

    byte[] bytes = new byte[insertRowNode.serializedSize()];
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));
    insertRowNode.serializeToWAL(walBuffer);
    Assert.assertFalse(walBuffer.getBuffer().hasRemaining());

    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), dataInputStream.readShort());

    InsertRowNode tmpNode = InsertRowNode.deserializeFromWAL(dataInputStream);
    Assert.assertArrayEquals(
        new String[] {"\u6e29\u5ea6", "s3", "s4", "s5"}, tmpNode.getMeasurements());
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
            new PartialPath("root.\u6570\u636e\u5e93.d2"),
            false,
            new String[] {"\u6e29\u5ea6", "\u6e7f\u5ea6", "s3", "s4", "s5"},
            dataTypes,
            time,
            columns,
            false);

    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("\u6e29\u5ea6", TSDataType.DOUBLE),
          new MeasurementSchema("\u6e7f\u5ea6", TSDataType.FLOAT),
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

  private RelationalInsertRowNode getRelationalInsertRowNodeWithMeasurementSchemas() {
    return new RelationalInsertRowNode(
        new PlanNodeId("plannode 3"),
        new PartialPath("table1", false),
        false,
        new String[] {"id", "attr", "value"},
        new TSDataType[] {TSDataType.STRING, TSDataType.TEXT, TSDataType.INT64},
        new MeasurementSchema[] {
          new MeasurementSchema("id", TSDataType.STRING),
          new MeasurementSchema("attr", TSDataType.TEXT),
          new MeasurementSchema("value", TSDataType.INT64)
        },
        90L,
        new Object[] {
          new Binary("d1".getBytes(StandardCharsets.UTF_8)),
          new Binary("v1".getBytes(StandardCharsets.UTF_8)),
          1L
        },
        false,
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG, TsTableColumnCategory.ATTRIBUTE, TsTableColumnCategory.FIELD
        });
  }
}
