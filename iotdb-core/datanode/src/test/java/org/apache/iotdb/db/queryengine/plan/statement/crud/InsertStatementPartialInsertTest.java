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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InsertStatementPartialInsertTest {

  @Test
  public void testInsertRowStatementGetPathsSkipsFailedMeasurements() throws Exception {
    final InsertRowStatement statement = createInsertRowStatement();
    statement.markFailedMeasurement(0, new RuntimeException("failed"));

    Assert.assertEquals(1, statement.getPaths().size());
    Assert.assertEquals("root.sg.d1.s2", statement.getPaths().get(0).getFullPath());
  }

  @Test
  public void testInsertTabletStatementGetPathsSkipsFailedMeasurements() throws Exception {
    final InsertTabletStatement statement = createInsertTabletStatement();
    statement.markFailedMeasurement(1, new RuntimeException("failed"));

    Assert.assertEquals(1, statement.getPaths().size());
    Assert.assertEquals("root.sg.d1.s1", statement.getPaths().get(0).getFullPath());
  }

  @Test
  public void testInsertRowsOfOneDeviceStatementSkipsFailedMeasurements() throws Exception {
    final InsertRowStatement statement = createInsertRowStatement();
    statement.markFailedMeasurement(0, new RuntimeException("failed"));
    final InsertRowsOfOneDeviceStatement rowsOfOneDeviceStatement =
        new InsertRowsOfOneDeviceStatement();
    rowsOfOneDeviceStatement.setInsertRowStatementList(Arrays.asList(statement));

    Assert.assertArrayEquals(new String[] {"s2"}, rowsOfOneDeviceStatement.getMeasurements());
    Assert.assertEquals(1, rowsOfOneDeviceStatement.getPaths().size());
    Assert.assertEquals("root.sg.d1.s2", rowsOfOneDeviceStatement.getPaths().get(0).getFullPath());
  }

  @Test
  public void testInsertRowStatementMarkFailedMeasurementHandlesMissingValue() throws Exception {
    final InsertRowStatement statement = createInsertRowStatement();
    statement.setValues(new Object[] {1});

    statement.markFailedMeasurement(1, new RuntimeException("failed"));

    Assert.assertNull(statement.getMeasurements()[1]);
    Assert.assertNull(statement.getDataTypes()[1]);

    statement.removeAllFailedMeasurementMarks();

    Assert.assertEquals("s2", statement.getMeasurements()[1]);
    Assert.assertEquals(TSDataType.INT64, statement.getDataTypes()[1]);
  }

  @Test
  public void testInsertTabletStatementMarkFailedMeasurementHandlesMissingColumn()
      throws Exception {
    final InsertTabletStatement statement = createInsertTabletStatement();
    statement.setColumns(new Object[] {new int[] {1, 2}});

    statement.markFailedMeasurement(1, new RuntimeException("failed"));

    Assert.assertNull(statement.getMeasurements()[1]);
    Assert.assertNull(statement.getDataTypes()[1]);

    statement.removeAllFailedMeasurementMarks();

    Assert.assertEquals("s2", statement.getMeasurements()[1]);
    Assert.assertEquals(TSDataType.INT64, statement.getDataTypes()[1]);
  }

  @Test
  public void testHasValidMeasurementsIgnoresNonFieldColumns() throws Exception {
    final InsertRowStatement statement = createInsertRowStatement();
    statement.setColumnCategories(
        new TsTableColumnCategory[] {TsTableColumnCategory.TAG, TsTableColumnCategory.ATTRIBUTE});

    Assert.assertFalse(statement.hasValidMeasurements());

    statement.setColumnCategories(
        new TsTableColumnCategory[] {TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD});

    Assert.assertTrue(statement.hasValidMeasurements());
  }

  @Test
  public void testInsertTabletStatementGetFirstValueOfIndexReturnsNullForNullColumn()
      throws Exception {
    final InsertTabletStatement statement = createInsertTabletStatement();
    statement.getColumns()[0] = null;

    Assert.assertNull(statement.getFirstValueOfIndex(0));
  }

  @Test
  public void testInsertTabletStatementTagAndAttributeIndicesSkipNullColumn() throws Exception {
    final InsertTabletStatement statement = createInsertTabletStatement();
    statement.setColumnCategories(
        new TsTableColumnCategory[] {TsTableColumnCategory.TAG, TsTableColumnCategory.ATTRIBUTE});
    statement.getColumns()[0] = null;

    Assert.assertTrue(statement.getTagColumnIndices().isEmpty());
    Assert.assertEquals(Arrays.asList(1), statement.getAttrColumnIndices());
    Assert.assertEquals(Arrays.asList("s2"), statement.getAttributeColumnNameList());
  }

  @Test
  public void testInsertRowStatementTableDeviceIDHandlesMissingTagValue() throws Exception {
    final InsertRowStatement statement = createInsertRowStatement();
    statement.setColumnCategories(
        new TsTableColumnCategory[] {TsTableColumnCategory.FIELD, TsTableColumnCategory.TAG});
    statement.getTagColumnIndices();
    statement.setValues(new Object[] {1});

    Assert.assertArrayEquals(
        new Object[] {"root.sg.d1"}, statement.getTableDeviceID().getSegments());
  }

  @Test
  public void testInsertTabletStatementTableDeviceIDUsesTagColumnBitmap() throws Exception {
    final InsertTabletStatement statement = createInsertTabletStatement();
    statement.setDataTypes(new TSDataType[] {TSDataType.INT32, TSDataType.STRING});
    statement.setColumnCategories(
        new TsTableColumnCategory[] {TsTableColumnCategory.FIELD, TsTableColumnCategory.TAG});
    statement.setColumns(
        new Object[] {
          new int[] {1, 2},
          new Binary[] {
            new Binary("tag1", StandardCharsets.UTF_8), new Binary("tag2", StandardCharsets.UTF_8)
          }
        });
    statement.setBitMaps(new BitMap[] {new BitMap(2, new byte[] {1}), new BitMap(2)});

    Assert.assertArrayEquals(
        new Object[] {"root.sg.d1", "tag1"}, statement.getTableDeviceID(0).getSegments());
  }

  @Test
  public void testInsertTabletStatementTableDeviceIDHandlesMissingTagColumn() throws Exception {
    final InsertTabletStatement statement = createInsertTabletStatement();
    statement.setColumnCategories(
        new TsTableColumnCategory[] {TsTableColumnCategory.FIELD, TsTableColumnCategory.TAG});
    statement.getTagColumnIndices();
    statement.setColumns(new Object[] {new int[] {1, 2}});

    Assert.assertArrayEquals(
        new Object[] {"root.sg.d1"}, statement.getTableDeviceID(0).getSegments());
  }

  @Test
  public void testGetDataTypeReturnsNullForShortTypeArray() throws Exception {
    final InsertRowStatement rowStatement = createInsertRowStatement();
    rowStatement.setDataTypes(new TSDataType[] {TSDataType.INT32});
    Assert.assertEquals(TSDataType.INT32, rowStatement.getDataType(0));
    Assert.assertNull(rowStatement.getDataType(1));

    final InsertTabletStatement tabletStatement = createInsertTabletStatement();
    tabletStatement.setDataTypes(new TSDataType[] {TSDataType.INT32});
    Assert.assertEquals(TSDataType.INT32, tabletStatement.getDataType(0));
    Assert.assertNull(tabletStatement.getDataType(1));
  }

  @Test
  public void testBaseSettersExpandShortArrays() throws Exception {
    final InsertRowStatement statement = createInsertRowStatement();
    statement.setDataTypes(new TSDataType[] {TSDataType.INT32});
    statement.setMeasurementSchemas(
        new MeasurementSchema[] {new MeasurementSchema("s1", TSDataType.INT32)});
    statement.setColumnCategories(new TsTableColumnCategory[] {TsTableColumnCategory.FIELD});

    statement.setDataType(TSDataType.INT64, 1);
    statement.setMeasurementSchema(new MeasurementSchema("s2", TSDataType.INT64), 1);
    statement.setColumnCategory(TsTableColumnCategory.TAG, 1);

    Assert.assertEquals(TSDataType.INT64, statement.getDataType(1));
    Assert.assertEquals("s2", statement.getMeasurementSchemas()[1].getMeasurementName());
    Assert.assertEquals(TsTableColumnCategory.TAG, statement.getColumnCategories()[1]);
  }

  @Test
  public void testInsertColumnWithNullDataTypesKeepsArraysAligned() throws Exception {
    final InsertRowStatement statement = createInsertRowStatement();
    statement.setDataTypes(null);

    final ColumnSchema columnSchema =
        new ColumnSchema(
            "tag1", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG);
    statement.insertColumn(1, columnSchema);

    Assert.assertEquals(3, statement.getMeasurements().length);
    Assert.assertEquals(3, statement.getDataTypes().length);
    Assert.assertEquals(3, statement.getValues().length);
    Assert.assertNull(statement.getDataType(0));
    Assert.assertEquals(TSDataType.STRING, statement.getDataType(1));
    Assert.assertNull(statement.getDataType(2));
  }

  @Test
  public void testRebuildArraysAfterExpansionHandlesShortValueArrays() throws Exception {
    final InsertRowStatement statement = createInsertRowStatement();
    statement.setDataTypes(new TSDataType[] {TSDataType.INT32});
    statement.setValues(new Object[] {1});
    statement.setColumnCategories(
        new TsTableColumnCategory[] {TsTableColumnCategory.FIELD, TsTableColumnCategory.FIELD});

    statement.rebuildArraysAfterExpansion(new int[] {-1, 0, 1}, new String[] {"tag1", "s1", "s2"});

    Assert.assertArrayEquals(new String[] {"tag1", "s1", "s2"}, statement.getMeasurements());
    Assert.assertArrayEquals(new Object[] {null, 1, null}, statement.getValues());
    Assert.assertEquals(TSDataType.STRING, statement.getDataType(0));
    Assert.assertEquals(TSDataType.INT32, statement.getDataType(1));
    Assert.assertNull(statement.getDataType(2));
    Assert.assertEquals(TsTableColumnCategory.TAG, statement.getColumnCategories()[0]);
  }

  @Test
  public void testInsertRowStatementSwapColumnInvalidatesTableDeviceId() throws Exception {
    final InsertRowStatement statement = createInsertRowStatement();
    statement.setColumnCategories(
        new TsTableColumnCategory[] {TsTableColumnCategory.TAG, TsTableColumnCategory.TAG});
    statement.setValues(new Object[] {"tag1", "tag2"});

    Assert.assertArrayEquals(
        new Object[] {"root.sg.d1", "tag1", "tag2"}, statement.getTableDeviceID().getSegments());

    statement.swapColumn(0, 1);

    Assert.assertArrayEquals(
        new Object[] {"root.sg.d1", "tag2", "tag1"}, statement.getTableDeviceID().getSegments());
  }

  @Test
  public void testDeviceMeasurementMapSkipsMissingRowValues() throws Exception {
    final InsertRowStatement statement = createInsertRowStatement();
    statement.setValues(new Object[] {1});

    final Map<PartialPath, List<Pair<String, Integer>>> result =
        statement.getMapFromDeviceToMeasurementAndIndex();

    Assert.assertEquals(1, result.get(statement.getDevicePath()).size());
    Assert.assertEquals("s1", result.get(statement.getDevicePath()).get(0).left);
  }

  @Test
  public void testDeviceMeasurementMapSkipsMissingTabletColumns() throws Exception {
    final InsertTabletStatement statement = createInsertTabletStatement();
    statement.setColumns(new Object[] {new int[] {1, 2}});

    final Map<PartialPath, List<Pair<String, Integer>>> result =
        statement.getMapFromDeviceToMeasurementAndIndex();

    Assert.assertEquals(1, result.get(statement.getDevicePath()).size());
    Assert.assertEquals("s1", result.get(statement.getDevicePath()).get(0).left);
  }

  private static InsertRowStatement createInsertRowStatement() throws Exception {
    final InsertRowStatement statement = new InsertRowStatement();
    statement.setDevicePath(new PartialPath("root.sg.d1"));
    statement.setMeasurements(new String[] {"s1", "s2"});
    statement.setDataTypes(new TSDataType[] {TSDataType.INT32, TSDataType.INT64});
    statement.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64)
        });
    statement.setValues(new Object[] {1, 2L});
    statement.setTime(1L);
    return statement;
  }

  private static InsertTabletStatement createInsertTabletStatement() throws Exception {
    final InsertTabletStatement statement = new InsertTabletStatement();
    statement.setDevicePath(new PartialPath("root.sg.d1"));
    statement.setMeasurements(new String[] {"s1", "s2"});
    statement.setDataTypes(new TSDataType[] {TSDataType.INT32, TSDataType.INT64});
    statement.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64)
        });
    statement.setColumns(new Object[] {new int[] {1, 2}, new long[] {3L, 4L}});
    statement.setTimes(new long[] {1L, 2L});
    statement.setRowCount(2);
    return statement;
  }
}
