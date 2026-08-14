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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterMatcher;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class ConsensusLogToTabletConverterTest {

  private static final String DATABASE_NAME = "db";

  @Test
  public void testConvertRelationalInsertRowNodeWithSingleMatchedColumn() {
    final ConsensusLogToTabletConverter converter = createConverter("id1");

    final List<Tablet> tablets = converter.convert(StatementTestUtils.genInsertRowNode(7));

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(StatementTestUtils.tableName(), tablet.getTableName());
    Assert.assertEquals(1, tablet.getRowSize());
    Assert.assertEquals(1, tablet.getSchemas().size());
    Assert.assertEquals("id1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, tablet.getColumnTypes().get(0));
    Assert.assertEquals("id:7", toUtf8(((Binary[]) tablet.getValues()[0])[0]));
  }

  @Test
  public void testConvertRelationalInsertRowNodeWithMultipleMatchedColumns() {
    final ConsensusLogToTabletConverter converter = createConverter("id1", "m1");

    final List<Tablet> tablets = converter.convert(StatementTestUtils.genInsertRowNode(9));

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(2, tablet.getSchemas().size());
    Assert.assertEquals("id1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("m1", tablet.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, tablet.getColumnTypes().get(0));
    Assert.assertEquals(ColumnCategory.FIELD, tablet.getColumnTypes().get(1));
    Assert.assertEquals("id:9", toUtf8(((Binary[]) tablet.getValues()[0])[0]));
    Assert.assertEquals(9.0, ((double[]) tablet.getValues()[1])[0], 0.0);
  }

  @Test
  public void testConvertRelationalInsertTabletNodeWithSingleMatchedColumn() {
    final ConsensusLogToTabletConverter converter = createConverter("m1");

    final RelationalInsertTabletNode node = StatementTestUtils.genInsertTabletNode(3, 10);
    final List<Tablet> tablets = converter.convert(node);

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(StatementTestUtils.tableName(), tablet.getTableName());
    Assert.assertEquals(3, tablet.getRowSize());
    Assert.assertEquals(2, tablet.getSchemas().size());
    Assert.assertEquals("id1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("m1", tablet.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, tablet.getColumnTypes().get(0));
    Assert.assertEquals(ColumnCategory.FIELD, tablet.getColumnTypes().get(1));
    Assert.assertArrayEquals(new long[] {10L, 11L, 12L}, tablet.getTimestamps());
    Assert.assertEquals("id:10", toUtf8(((Binary[]) tablet.getValues()[0])[0]));
    Assert.assertArrayEquals(
        new double[] {10.0, 11.0, 12.0}, (double[]) tablet.getValues()[1], 0.0);
    Assert.assertSame(node.getTimes(), tablet.getTimestamps());
    Assert.assertSame(node.getColumns()[0], tablet.getValues()[0]);
    Assert.assertSame(node.getColumns()[2], tablet.getValues()[1]);
  }

  @Test
  public void testConvertRelationalInsertTabletNodeConvertsDateForSerialization() throws Exception {
    final ConsensusLogToTabletConverter converter = createConverter("value");
    final LocalDate date = LocalDate.of(2026, 1, 31);
    final RelationalInsertTabletNode node =
        new RelationalInsertTabletNode(
            new PlanNodeId("date"),
            new PartialPath(new String[] {StatementTestUtils.tableName()}),
            true,
            new String[] {"device_id", "value"},
            new TSDataType[] {TSDataType.STRING, TSDataType.DATE},
            new long[] {1L},
            null,
            new Object[] {
              new Binary[] {new Binary("device_0".getBytes(StandardCharsets.UTF_8))},
              new int[] {DateUtils.parseDateExpressionToInt(date)}
            },
            1,
            new TsTableColumnCategory[] {TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD});

    final List<Tablet> tablets = converter.convert(node);

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertArrayEquals(new LocalDate[] {date}, (LocalDate[]) tablet.getValues()[1]);
    final Tablet deserializedTablet = Tablet.deserialize(tablet.serialize());
    Assert.assertArrayEquals(
        new LocalDate[] {date}, (LocalDate[]) deserializedTablet.getValues()[1]);
  }

  @Test
  public void testConvertRelationalInsertTabletNodeKeepsNullMatchedFieldColumn() {
    final ConsensusLogToTabletConverter converter = createConverter("m1");

    final RelationalInsertTabletNode node = StatementTestUtils.genInsertTabletNode(3, 10);
    node.getColumns()[2] = null;
    final BitMap[] bitMaps = new BitMap[] {null, null, new BitMap(3)};
    bitMaps[2].mark(0);
    bitMaps[2].mark(1);
    bitMaps[2].mark(2);
    node.setBitMaps(bitMaps);

    final List<Tablet> tablets = converter.convert(node);

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(2, tablet.getSchemas().size());
    Assert.assertEquals("id1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("m1", tablet.getSchemas().get(1).getMeasurementName());
    Assert.assertNull(tablet.getValues()[1]);
    Assert.assertTrue(tablet.getBitMaps()[1].isMarked(0));
    Assert.assertTrue(tablet.getBitMaps()[1].isMarked(1));
    Assert.assertTrue(tablet.getBitMaps()[1].isMarked(2));
  }

  @Test
  public void testConvertRelationalInsertTabletNodeSkipsNullTagColumn() {
    final ConsensusLogToTabletConverter converter = createConverter("m1");

    final RelationalInsertTabletNode node = StatementTestUtils.genInsertTabletNode(3, 10);
    node.getColumns()[0] = null;
    final List<Tablet> tablets = converter.convert(node);

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(1, tablet.getSchemas().size());
    Assert.assertEquals("m1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals(ColumnCategory.FIELD, tablet.getColumnTypes().get(0));
    Assert.assertArrayEquals(
        new double[] {10.0, 11.0, 12.0}, (double[]) tablet.getValues()[0], 0.0);
  }

  @Test
  public void testConvertRelationalInsertTabletNodeSkipsMalformedColumns()
      throws IllegalPathException {
    final ConsensusLogToTabletConverter converter = createConverter("id1");
    final RelationalInsertTabletNode node =
        new RelationalInsertTabletNode(
            new PlanNodeId("malformed"),
            new PartialPath(new String[] {StatementTestUtils.tableName()}),
            true,
            new String[] {"id1", "m1"},
            new TSDataType[] {TSDataType.STRING},
            new long[] {1L},
            null,
            new Object[] {new Binary[] {new Binary("id:1".getBytes(StandardCharsets.UTF_8))}},
            1,
            StatementTestUtils.genColumnCategories());

    final List<Tablet> tablets = converter.convert(node);

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(1, tablet.getSchemas().size());
    Assert.assertEquals("id1", tablet.getSchemas().get(0).getMeasurementName());
  }

  @Test
  public void testConvertRelationalInsertRowNodeKeepsTagColumnsForMatchedField() {
    final ConsensusLogToTabletConverter converter = createConverter("m1");

    final List<Tablet> tablets = converter.convert(StatementTestUtils.genInsertRowNode(11));

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(2, tablet.getSchemas().size());
    Assert.assertEquals("id1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("m1", tablet.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, tablet.getColumnTypes().get(0));
    Assert.assertEquals(ColumnCategory.FIELD, tablet.getColumnTypes().get(1));
    Assert.assertEquals("id:11", toUtf8(((Binary[]) tablet.getValues()[0])[0]));
    Assert.assertEquals(11.0, ((double[]) tablet.getValues()[1])[0], 0.0);
  }

  @Test
  public void testConvertRelationalInsertRowNodeKeepsNullMatchedFieldWithBitmap() {
    final ConsensusLogToTabletConverter converter = createConverter("m1");
    final RelationalInsertRowNode node = StatementTestUtils.genInsertRowNode(12);
    node.getValues()[2] = null;

    final List<Tablet> tablets = converter.convert(node);

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(2, tablet.getSchemas().size());
    Assert.assertEquals("id1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("m1", tablet.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals("id:12", toUtf8(((Binary[]) tablet.getValues()[0])[0]));
    Assert.assertTrue(tablet.getBitMaps()[1].isMarked(0));
  }

  @Test
  public void testConvertRelationalInsertNodeReturnsEmptyWhenNoColumnsMatch() {
    final ConsensusLogToTabletConverter converter = createConverter("not_exist");

    Assert.assertTrue(converter.convert(StatementTestUtils.genInsertRowNode(0)).isEmpty());
    Assert.assertTrue(converter.convert(StatementTestUtils.genInsertTabletNode(2, 0)).isEmpty());
  }

  @Test
  public void testConvertInsertRowsOfOneDeviceNodeGroupsRowsWithSameSchema()
      throws IllegalPathException {
    final ConsensusLogToTabletConverter converter = createTreeConverter();
    final InsertRowsOfOneDeviceNode node = new InsertRowsOfOneDeviceNode(new PlanNodeId("plan"));
    final List<InsertRowNode> rows = new ArrayList<>();
    final List<Integer> rowIndexes = new ArrayList<>();
    rows.add(createTreeRow("root.sg.d1", 1L, new Object[] {1, 1.0}));
    rows.add(createTreeRow("root.sg.d1", 2L, new Object[] {2, null}));
    rowIndexes.add(0);
    rowIndexes.add(1);
    node.setInsertRowNodeList(rows);
    node.setInsertRowNodeIndexList(rowIndexes);

    final List<Tablet> tablets = converter.convert(node);

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals("root.sg.d1", tablet.getDeviceId());
    Assert.assertEquals(2, tablet.getRowSize());
    Assert.assertArrayEquals(new long[] {1L, 2L}, tablet.getTimestamps());
    Assert.assertArrayEquals(new int[] {1, 2}, (int[]) tablet.getValues()[0]);
    Assert.assertEquals(1.0, ((double[]) tablet.getValues()[1])[0], 0.0);
    Assert.assertTrue(tablet.getBitMaps()[1].isMarked(1));
  }

  @Test
  public void testConvertInsertRowsNodeKeepsOrderWhenGroupingTreeRows()
      throws IllegalPathException {
    final ConsensusLogToTabletConverter converter = createTreeConverter();
    final InsertRowsNode node = new InsertRowsNode(new PlanNodeId("plan"));
    node.addOneInsertRowNode(createTreeRow("root.sg.d1", 1L, new Object[] {1, 1.0}), 0);
    node.addOneInsertRowNode(createTreeRow("root.sg.d1", 2L, new Object[] {2, 2.0}), 1);
    node.addOneInsertRowNode(createTreeRow("root.sg.d2", 3L, new Object[] {3, 3.0}), 2);
    node.addOneInsertRowNode(createTreeRow("root.sg.d1", 4L, new Object[] {4, 4.0}), 3);

    final List<Tablet> tablets = converter.convert(node);

    Assert.assertEquals(3, tablets.size());
    Assert.assertEquals("root.sg.d1", tablets.get(0).getDeviceId());
    Assert.assertEquals(2, tablets.get(0).getRowSize());
    Assert.assertArrayEquals(new long[] {1L, 2L}, tablets.get(0).getTimestamps());
    Assert.assertEquals("root.sg.d2", tablets.get(1).getDeviceId());
    Assert.assertEquals(1, tablets.get(1).getRowSize());
    Assert.assertEquals("root.sg.d1", tablets.get(2).getDeviceId());
    Assert.assertEquals(1, tablets.get(2).getRowSize());
  }

  @Test
  public void testConvertTreeInsertTabletNodeSkipsNullColumn() throws IllegalPathException {
    final ConsensusLogToTabletConverter converter = createTreeConverter();
    final InsertTabletNode node =
        new InsertTabletNode(
            new PlanNodeId("tablet"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.DOUBLE},
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.DOUBLE)
            },
            new long[] {1L, 2L},
            null,
            new Object[] {new int[] {1, 2}, null},
            2);

    final List<Tablet> tablets = converter.convert(node);

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals("root.sg.d1", tablet.getDeviceId());
    Assert.assertEquals(1, tablet.getSchemas().size());
    Assert.assertEquals("s1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertArrayEquals(new int[] {1, 2}, (int[]) tablet.getValues()[0]);
  }

  private static ConsensusLogToTabletConverter createConverter(final String... selectedColumns) {
    return new ConsensusLogToTabletConverter(
        null,
        new TablePattern(true, DATABASE_NAME, StatementTestUtils.tableName()),
        ColumnFilterMatcher.ofSelectedColumnNames(new HashSet<>(Arrays.asList(selectedColumns))),
        DATABASE_NAME);
  }

  private static ConsensusLogToTabletConverter createTreeConverter() {
    return new ConsensusLogToTabletConverter(new IoTDBTreePattern("root.sg.**"), null, null, null);
  }

  private static InsertRowNode createTreeRow(
      final String devicePath, final long time, final Object[] values) throws IllegalPathException {
    return new InsertRowNode(
        new PlanNodeId("row"),
        new PartialPath(devicePath),
        false,
        new String[] {"s1", "s2"},
        new TSDataType[] {TSDataType.INT32, TSDataType.DOUBLE},
        time,
        values,
        false);
  }

  private static String toUtf8(final Binary value) {
    return new String(value.getValues(), StandardCharsets.UTF_8);
  }
}
