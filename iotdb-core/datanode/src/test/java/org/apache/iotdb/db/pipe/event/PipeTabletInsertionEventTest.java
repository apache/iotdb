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

package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.pipe.event.common.row.PipeResetTabletRow;
import org.apache.iotdb.db.pipe.event.common.row.PipeRowCollector;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventTablePatternParser;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventTreePatternParser;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern.buildUnionPattern;

public class PipeTabletInsertionEventTest {

  private InsertRowNode insertRowNode;
  private InsertRowNode insertRowNodeAligned;
  private InsertTabletNode insertTabletNode;
  private InsertTabletNode insertTabletNodeAligned;

  final String deviceId = "root.sg.d1";
  final long[] times = new long[] {110L, 111L, 112L, 113L, 114L};
  final String[] measurementIds =
      new String[] {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};
  final TSDataType[] dataTypes =
      new TSDataType[] {
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.BOOLEAN,
        TSDataType.TEXT,
        TSDataType.TIMESTAMP,
        TSDataType.DATE,
        TSDataType.BLOB,
        TSDataType.STRING,
      };

  final MeasurementSchema[] schemas = new MeasurementSchema[10];

  final String pattern = "root.sg.d1";

  Tablet tabletForInsertRowNode;
  Tablet tabletForInsertTabletNode;

  @Before
  public void setUp() throws Exception {
    createMeasurementSchema();
    createInsertRowNode();
    createInsertTabletNode();
    createTablet();
  }

  private void createMeasurementSchema() {
    for (int i = 0; i < schemas.length; i++) {
      schemas[i] = new MeasurementSchema(measurementIds[i], dataTypes[i]);
    }
  }

  private void createInsertRowNode() throws IllegalPathException {
    final Object[] values = new Object[schemas.length];

    values[0] = 100;
    values[1] = 10000L;
    values[2] = 2F;
    values[3] = 1D;
    values[4] = false;
    values[5] = BytesUtils.valueOf("text");
    values[6] = 20000L;
    values[7] = LocalDate.of(2024, 1, 1);
    values[8] = BytesUtils.valueOf("blob");
    values[9] = BytesUtils.valueOf("string");

    insertRowNode =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath(deviceId),
            false,
            measurementIds,
            dataTypes,
            schemas,
            times[0],
            values,
            false);

    insertRowNodeAligned =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath(deviceId),
            true,
            measurementIds,
            dataTypes,
            schemas,
            times[0],
            values,
            false);
  }

  private void createInsertTabletNode() throws IllegalPathException {
    final Object[] values = new Object[schemas.length];

    values[0] = new int[times.length];
    values[1] = new long[times.length];
    values[2] = new float[times.length];
    values[3] = new double[times.length];
    values[4] = new boolean[times.length];
    values[5] = new Binary[times.length];
    values[6] = new long[times.length];
    values[7] = new LocalDate[times.length];
    values[8] = new Binary[times.length];
    values[9] = new Binary[times.length];

    for (int r = 0; r < times.length; r++) {
      ((int[]) values[0])[r] = 100;
      ((long[]) values[1])[r] = 10000L;
      ((float[]) values[2])[r] = 2F;
      ((double[]) values[3])[r] = 1D;
      ((boolean[]) values[4])[r] = false;
      ((Binary[]) values[5])[r] = BytesUtils.valueOf("text");
      ((long[]) values[6])[r] = 20000L;
      ((LocalDate[]) values[7])[r] = LocalDate.of(2024, 1, 1);
      ((Binary[]) values[8])[r] = BytesUtils.valueOf("blob");
      ((Binary[]) values[9])[r] = BytesUtils.valueOf("string");
    }

    this.insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("plannode 1"),
            new PartialPath(deviceId),
            false,
            measurementIds,
            dataTypes,
            schemas,
            times,
            null,
            values,
            times.length);

    this.insertTabletNodeAligned =
        new InsertTabletNode(
            new PlanNodeId("plannode 1"),
            new PartialPath(deviceId),
            true,
            measurementIds,
            dataTypes,
            schemas,
            times,
            null,
            values,
            times.length);
  }

  private void createTablet() {
    final Object[] values = new Object[schemas.length];

    // create tablet for insertRowNode
    values[0] = new int[1];
    values[1] = new long[1];
    values[2] = new float[1];
    values[3] = new double[1];
    values[4] = new boolean[1];
    values[5] = new Binary[1];
    values[6] = new long[times.length];
    values[7] = new LocalDate[times.length];
    values[8] = new Binary[times.length];
    values[9] = new Binary[times.length];

    for (int r = 0; r < 1; r++) {
      ((int[]) values[0])[r] = 100;
      ((long[]) values[1])[r] = 10000L;
      ((float[]) values[2])[r] = 2F;
      ((double[]) values[3])[r] = 1D;
      ((boolean[]) values[4])[r] = false;
      ((Binary[]) values[5])[r] = BytesUtils.valueOf("text");
      ((long[]) values[6])[r] = 20000L;
      ((LocalDate[]) values[7])[r] = LocalDate.of(2024, 1, 1);
      ((Binary[]) values[8])[r] = BytesUtils.valueOf("blob");
      ((Binary[]) values[9])[r] = BytesUtils.valueOf("string");
    }

    tabletForInsertRowNode =
        new Tablet(deviceId, Arrays.asList(schemas), new long[] {times[0]}, values, null, 1);

    // create tablet for insertTabletNode
    values[0] = new int[times.length];
    values[1] = new long[times.length];
    values[2] = new float[times.length];
    values[3] = new double[times.length];
    values[4] = new boolean[times.length];
    values[5] = new Binary[times.length];
    values[6] = new long[times.length];
    values[7] = new LocalDate[times.length];
    values[8] = new Binary[times.length];
    values[9] = new Binary[times.length];

    for (int r = 0; r < times.length; r++) {
      ((int[]) values[0])[r] = 100;
      ((long[]) values[1])[r] = 10000L;
      ((float[]) values[2])[r] = 2F;
      ((double[]) values[3])[r] = 1D;
      ((boolean[]) values[4])[r] = false;
      ((Binary[]) values[5])[r] = BytesUtils.valueOf("text");
      ((long[]) values[6])[r] = 20000L;
      ((LocalDate[]) values[7])[r] = LocalDate.of(2024, 1, 1);
      ((Binary[]) values[8])[r] = BytesUtils.valueOf("blob");
      ((Binary[]) values[9])[r] = BytesUtils.valueOf("string");
    }

    tabletForInsertTabletNode = new Tablet(deviceId, Arrays.asList(schemas), times.length);
    tabletForInsertTabletNode =
        new Tablet(deviceId, Arrays.asList(schemas), times, values, null, times.length);
  }

  @Test
  public void convertToTabletForTest() throws Exception {
    TabletInsertionEventTreePatternParser container1 =
        new TabletInsertionEventTreePatternParser(insertRowNode, new PrefixTreePattern(pattern));
    Tablet tablet1 = container1.convertToTablet();
    boolean isAligned1 = container1.isAligned();
    Assert.assertEquals(tablet1, tabletForInsertRowNode);
    Assert.assertFalse(isAligned1);

    TabletInsertionEventTreePatternParser container2 =
        new TabletInsertionEventTreePatternParser(insertTabletNode, new PrefixTreePattern(pattern));
    Tablet tablet2 = container2.convertToTablet();
    boolean isAligned2 = container2.isAligned();
    Assert.assertEquals(tablet2, tabletForInsertTabletNode);
    Assert.assertFalse(isAligned2);

    PipeRawTabletInsertionEvent event3 =
        new PipeRawTabletInsertionEvent(tablet1, false, new PrefixTreePattern(pattern));
    Tablet tablet3 = event3.convertToTablet();
    boolean isAligned3 = event3.isAligned();
    Assert.assertEquals(tablet1, tablet3);
    Assert.assertFalse(isAligned3);

    PipeRawTabletInsertionEvent event4 =
        new PipeRawTabletInsertionEvent(tablet2, false, new PrefixTreePattern(pattern));
    Tablet tablet4 = event4.convertToTablet();
    boolean isAligned4 = event4.isAligned();
    Assert.assertEquals(tablet2, tablet4);
    Assert.assertFalse(isAligned4);
  }

  @Test
  public void convertToAlignedTabletForTest() throws Exception {
    TabletInsertionEventTreePatternParser container1 =
        new TabletInsertionEventTreePatternParser(
            insertRowNodeAligned, new PrefixTreePattern(pattern));
    Tablet tablet1 = container1.convertToTablet();
    boolean isAligned1 = container1.isAligned();
    Assert.assertEquals(tablet1, tabletForInsertRowNode);
    Assert.assertTrue(isAligned1);

    TabletInsertionEventTreePatternParser container2 =
        new TabletInsertionEventTreePatternParser(
            insertTabletNodeAligned, new PrefixTreePattern(pattern));
    Tablet tablet2 = container2.convertToTablet();
    boolean isAligned2 = container2.isAligned();
    Assert.assertEquals(tablet2, tabletForInsertTabletNode);
    Assert.assertTrue(isAligned2);

    PipeRawTabletInsertionEvent event3 =
        new PipeRawTabletInsertionEvent(tablet1, true, new PrefixTreePattern(pattern));
    Tablet tablet3 = event3.convertToTablet();
    boolean isAligned3 = event3.isAligned();
    Assert.assertEquals(tablet1, tablet3);
    Assert.assertTrue(isAligned3);

    PipeRawTabletInsertionEvent event4 =
        new PipeRawTabletInsertionEvent(tablet2, true, new PrefixTreePattern(pattern));
    Tablet tablet4 = event4.convertToTablet();
    boolean isAligned4 = event4.isAligned();
    Assert.assertEquals(tablet2, tablet4);
    Assert.assertTrue(isAligned4);
  }

  @Test
  public void processAlignedTabletWithCollectPreservesAlignmentForTest() {
    final PipeRawTabletInsertionEvent event =
        new PipeRawTabletInsertionEvent(
            tabletForInsertTabletNode, true, new PrefixTreePattern(pattern));

    final List<TabletInsertionEvent> events = new ArrayList<>();
    event
        .processTabletWithCollect(
            (tablet, collector) -> {
              try {
                collector.collectTablet(tablet);
              } catch (final Exception e) {
                throw new RuntimeException(e);
              }
            })
        .forEach(events::add);

    Assert.assertEquals(1, events.size());
    final PipeRawTabletInsertionEvent collectedEvent = (PipeRawTabletInsertionEvent) events.get(0);
    Assert.assertEquals(tabletForInsertTabletNode, collectedEvent.convertToTablet());
    Assert.assertTrue(collectedEvent.isAligned());
  }

  @Test
  public void collectRowWithOverriddenTreeDatabaseForTest() {
    final PipeRowCollector rowCollector = new PipeRowCollector(null, null, "root.test.sg_0", false);
    rowCollector.resetDatabaseInfo("root.userResultDB", false, null, "root.userResultDB");

    final MeasurementSchema[] outputSchemas = {new MeasurementSchema("avg", TSDataType.INT32)};
    rowCollector.collectRow(
        new PipeResetTabletRow(
            0,
            "root.userResultDB.d_0.s_1",
            false,
            outputSchemas,
            new long[] {1L},
            new TSDataType[] {TSDataType.INT32},
            new Object[] {new int[] {1}},
            null,
            new String[] {"avg"}));

    final List<org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent> events =
        rowCollector.convertToTabletInsertionEvents(false);
    Assert.assertEquals(1, events.size());

    final PipeRawTabletInsertionEvent event = (PipeRawTabletInsertionEvent) events.get(0);
    Assert.assertEquals("root.userResultDB", event.getSourceDatabaseNameFromDataRegion());
    Assert.assertFalse(event.isTableModelEvent());
    Assert.assertEquals("root.userResultDB", event.getTreeModelDatabaseName());
    Assert.assertEquals("root.userResultDB.d_0.s_1", event.convertToTablet().getDeviceId());
  }

  @Test
  public void convertToTabletSkipsUnnecessaryBitMapsForTest() throws Exception {
    final BitMap[] bitMaps = new BitMap[schemas.length];
    bitMaps[0] = new BitMap(times.length);
    bitMaps[1] = new BitMap(times.length);
    bitMaps[1].mark(1);

    final InsertTabletNode nodeWithSparseColumn =
        new InsertTabletNode(
            new PlanNodeId("plannode bitmap"),
            new PartialPath(deviceId),
            false,
            measurementIds,
            dataTypes,
            schemas,
            times,
            bitMaps,
            insertTabletNode.getColumns(),
            times.length);

    final Tablet tablet =
        new TabletInsertionEventTreePatternParser(
                nodeWithSparseColumn, new PrefixTreePattern(pattern))
            .convertToTablet();

    Assert.assertNotNull(tablet.getBitMaps());
    Assert.assertNull(tablet.getBitMaps()[0]);
    Assert.assertNotNull(tablet.getBitMaps()[1]);
    Assert.assertTrue(tablet.isNull(1, 1));
  }

  @Test
  public void convertToTabletSkipsFailedMeasurementsForCoveredTreePattern() throws Exception {
    final InsertRowNode rowNode =
        new InsertRowNode(
            new PlanNodeId("plan node failed row"),
            new PartialPath(deviceId),
            false,
            Arrays.copyOf(measurementIds, measurementIds.length),
            Arrays.copyOf(dataTypes, dataTypes.length),
            Arrays.copyOf(schemas, schemas.length),
            times[0],
            Arrays.copyOf(insertRowNode.getValues(), schemas.length),
            false);
    rowNode.markFailedMeasurement(1);

    final Tablet rowTablet =
        new TabletInsertionEventTreePatternParser(rowNode, new PrefixTreePattern(pattern))
            .convertToTablet();
    assertTabletDoesNotContainMeasurement(rowTablet, "s2", schemas.length - 1);

    final InsertTabletNode tabletNode =
        new InsertTabletNode(
            new PlanNodeId("plan node failed tablet"),
            new PartialPath(deviceId),
            false,
            Arrays.copyOf(measurementIds, measurementIds.length),
            Arrays.copyOf(dataTypes, dataTypes.length),
            Arrays.copyOf(schemas, schemas.length),
            times,
            null,
            Arrays.copyOf(insertTabletNode.getColumns(), schemas.length),
            times.length);
    tabletNode.markFailedMeasurement(1);

    final Tablet tablet =
        new TabletInsertionEventTreePatternParser(tabletNode, new PrefixTreePattern(pattern))
            .convertToTablet();
    assertTabletDoesNotContainMeasurement(tablet, "s2", schemas.length - 1);
  }

  @Test
  public void convertToTabletSkipsFailedMeasurementsForTablePattern() throws Exception {
    final TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[schemas.length];
    Arrays.fill(columnCategories, TsTableColumnCategory.FIELD);
    columnCategories[0] = TsTableColumnCategory.TAG;
    columnCategories[2] = TsTableColumnCategory.ATTRIBUTE;

    final RelationalInsertTabletNode tabletNode =
        new RelationalInsertTabletNode(
            new PlanNodeId("plan node failed relational tablet"),
            new PartialPath("table1", false),
            false,
            Arrays.copyOf(measurementIds, measurementIds.length),
            Arrays.copyOf(dataTypes, dataTypes.length),
            times,
            null,
            Arrays.copyOf(insertTabletNode.getColumns(), schemas.length),
            times.length,
            columnCategories);
    tabletNode.setMeasurementSchemas(Arrays.copyOf(schemas, schemas.length));
    tabletNode.markFailedMeasurement(1);

    final Tablet tablet =
        new TabletInsertionEventTablePatternParser(
                null, null, tabletNode, new TablePattern(true, null, null))
            .convertToTablet();
    assertTabletDoesNotContainMeasurement(tablet, "s2", schemas.length - 1);
  }

  @Test
  public void convertToTabletWithFilteredRowsForTest() throws Exception {
    TabletInsertionEventTreePatternParser container1 =
        new TabletInsertionEventTreePatternParser(
            null,
            new PipeRawTabletInsertionEvent(tabletForInsertRowNode, 111L, 113L),
            insertRowNode,
            new PrefixTreePattern(pattern),
            null);
    Tablet tablet1 = container1.convertToTablet();
    Assert.assertEquals(0, tablet1.getRowSize());
    boolean isAligned1 = container1.isAligned();
    Assert.assertFalse(isAligned1);

    TabletInsertionEventTreePatternParser container2 =
        new TabletInsertionEventTreePatternParser(
            null,
            new PipeRawTabletInsertionEvent(tabletForInsertRowNode, 110L, 110L),
            insertRowNode,
            new PrefixTreePattern(pattern),
            null);
    Tablet tablet2 = container2.convertToTablet();
    Assert.assertEquals(1, tablet2.getRowSize());
    boolean isAligned2 = container2.isAligned();
    Assert.assertFalse(isAligned2);

    TabletInsertionEventTreePatternParser container3 =
        new TabletInsertionEventTreePatternParser(
            null,
            new PipeRawTabletInsertionEvent(tabletForInsertTabletNode, 111L, 113L),
            insertTabletNode,
            new PrefixTreePattern(pattern),
            null);
    Tablet tablet3 = container3.convertToTablet();
    Assert.assertEquals(3, tablet3.getRowSize());
    boolean isAligned3 = container3.isAligned();
    Assert.assertFalse(isAligned3);

    TabletInsertionEventTreePatternParser container4 =
        new TabletInsertionEventTreePatternParser(
            null,
            new PipeRawTabletInsertionEvent(tabletForInsertTabletNode, Long.MIN_VALUE, 109L),
            insertTabletNode,
            new PrefixTreePattern(pattern),
            null);
    Tablet tablet4 = container4.convertToTablet();
    Assert.assertEquals(0, tablet4.getRowSize());
    boolean isAligned4 = container4.isAligned();
    Assert.assertFalse(isAligned4);
  }

  @Test
  public void isEventTimeOverlappedWithTimeRangeTest() {
    PipeRawTabletInsertionEvent event;

    event = new PipeRawTabletInsertionEvent(tabletForInsertRowNode, 111L, 113L);
    Assert.assertFalse(event.mayEventTimeOverlappedWithTimeRange());
    event = new PipeRawTabletInsertionEvent(tabletForInsertRowNode, 110L, 110L);
    Assert.assertTrue(event.mayEventTimeOverlappedWithTimeRange());

    event = new PipeRawTabletInsertionEvent(tabletForInsertTabletNode, 111L, 113L);
    Assert.assertTrue(event.mayEventTimeOverlappedWithTimeRange());
    event = new PipeRawTabletInsertionEvent(tabletForInsertTabletNode, Long.MIN_VALUE, 110L);
    Assert.assertTrue(event.mayEventTimeOverlappedWithTimeRange());
    event = new PipeRawTabletInsertionEvent(tabletForInsertTabletNode, 114L, Long.MAX_VALUE);
    Assert.assertTrue(event.mayEventTimeOverlappedWithTimeRange());
    event = new PipeRawTabletInsertionEvent(tabletForInsertTabletNode, Long.MIN_VALUE, 109L);
    Assert.assertFalse(event.mayEventTimeOverlappedWithTimeRange());
    event = new PipeRawTabletInsertionEvent(tabletForInsertTabletNode, 115L, Long.MAX_VALUE);
    Assert.assertFalse(event.mayEventTimeOverlappedWithTimeRange());
  }

  @Test
  public void isEventTimeOverlappedWithTimeRangeUsesActualRowSizeForTest() throws Exception {
    final long[] timestamps = new long[] {110L, 111L, 112L, 0L, 0L};

    final Tablet partialTablet = new Tablet(deviceId, Arrays.asList(schemas), times.length);
    partialTablet.setTimestamps(timestamps);
    partialTablet.setRowSize(3);

    PipeRawTabletInsertionEvent rawEvent =
        new PipeRawTabletInsertionEvent(partialTablet, 111L, 112L);
    Assert.assertTrue(rawEvent.mayEventTimeOverlappedWithTimeRange());
    rawEvent = new PipeRawTabletInsertionEvent(partialTablet, 113L, Long.MAX_VALUE);
    Assert.assertFalse(rawEvent.mayEventTimeOverlappedWithTimeRange());

    final InsertTabletNode partialInsertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("partial tablet node"),
            new PartialPath(deviceId),
            false,
            measurementIds,
            dataTypes,
            schemas,
            timestamps,
            null,
            insertTabletNode.getColumns(),
            3);

    final Tablet convertedTablet =
        new TabletInsertionEventTreePatternParser(
                partialInsertTabletNode, new PrefixTreePattern(pattern))
            .convertToTablet();
    Assert.assertEquals(3, convertedTablet.getRowSize());
    Assert.assertArrayEquals(
        new long[] {110L, 111L, 112L},
        Arrays.copyOf(convertedTablet.getTimestamps(), convertedTablet.getRowSize()));

    PipeInsertNodeTabletInsertionEvent insertNodeEvent =
        new PipeInsertNodeTabletInsertionEvent(
            false,
            "root.sg",
            partialInsertTabletNode,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            111L,
            112L);
    Assert.assertTrue(insertNodeEvent.mayEventTimeOverlappedWithTimeRange());
    insertNodeEvent =
        new PipeInsertNodeTabletInsertionEvent(
            false,
            "root.sg",
            partialInsertTabletNode,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            113L,
            Long.MAX_VALUE);
    Assert.assertFalse(insertNodeEvent.mayEventTimeOverlappedWithTimeRange());
  }

  @Test
  public void testAuthCheck() {
    PipeInsertNodeTabletInsertionEvent event;

    event =
        new PipeInsertNodeTabletInsertionEvent(
            false,
            "root.db",
            insertRowNode,
            null,
            0,
            null,
            buildUnionPattern(false, Collections.singletonList(new IoTDBTreePattern(false, null))),
            new TablePattern(true, null, null),
            "0",
            "user",
            "localhost",
            false,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    final AccessControl oldControl = AuthorityChecker.getAccessControl();
    try {
      AuthorityChecker.setAccessControl(new PipeTsFileInsertionEventTest.TestAccessControl());
      Assert.assertThrows(AccessDeniedException.class, event::throwIfNoPrivilege);

      event =
          new PipeInsertNodeTabletInsertionEvent(
              true,
              "db",
              new RelationalInsertRowNode(
                  new PlanNodeId("plan node 1"),
                  new PartialPath("tb", false),
                  false,
                  new String[] {"s1", "s4"},
                  new TSDataType[] {TSDataType.DOUBLE, TSDataType.BOOLEAN},
                  2000L,
                  new Object[] {2.0, false},
                  false,
                  new TsTableColumnCategory[] {
                    TsTableColumnCategory.TAG,
                    TsTableColumnCategory.ATTRIBUTE,
                    TsTableColumnCategory.FIELD
                  }),
              null,
              0,
              null,
              buildUnionPattern(
                  false, Collections.singletonList(new IoTDBTreePattern(false, null))),
              new TablePattern(true, null, null),
              "0",
              "user",
              "localhost",
              false,
              Long.MIN_VALUE,
              Long.MAX_VALUE);
      Assert.assertThrows(AccessDeniedException.class, event::throwIfNoPrivilege);
    } finally {
      AuthorityChecker.setAccessControl(oldControl);
      event.close();
    }
  }

  @Test
  public void testAuthCheckIgnoresNullMeasurementInPartialInsert() throws Exception {
    insertRowNode.markFailedMeasurement(1);

    final PipeInsertNodeTabletInsertionEvent event =
        new PipeInsertNodeTabletInsertionEvent(
            false,
            "root.db",
            insertRowNode,
            null,
            0,
            null,
            new PrefixTreePattern(pattern),
            new TablePattern(true, null, null),
            "0",
            "user",
            "localhost",
            false,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    final AccessControl oldControl = AuthorityChecker.getAccessControl();
    final NullMeasurementRejectingAccessControl accessControl =
        new NullMeasurementRejectingAccessControl();
    try {
      AuthorityChecker.setAccessControl(accessControl);

      event.throwIfNoPrivilege();

      Assert.assertFalse(accessControl.hasNullMeasurementPath);
      Assert.assertFalse(event.shouldParse4Privilege());
    } finally {
      AuthorityChecker.setAccessControl(oldControl);
      event.close();
    }
  }

  private static class NullMeasurementRejectingAccessControl
      extends PipeTsFileInsertionEventTest.TestAccessControl {

    private boolean hasNullMeasurementPath = false;

    @Override
    public TSStatus checkSeriesPrivilege4Pipe(
        final IAuditEntity context,
        final java.util.List<? extends PartialPath> checkedPathsSupplier,
        final PrivilegeType permission) {
      hasNullMeasurementPath =
          checkedPathsSupplier.stream().anyMatch(path -> path.getFullPath().endsWith(".null"));
      return hasNullMeasurementPath
          ? AuthorityChecker.getTSStatus(
              Collections.singletonList(0), checkedPathsSupplier, permission)
          : new TSStatus(org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
  }

  private static void assertTabletDoesNotContainMeasurement(
      final Tablet tablet, final String measurement, final int expectedSchemaSize) {
    Assert.assertEquals(expectedSchemaSize, tablet.getSchemas().size());
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      Assert.assertNotNull(tablet.getSchemas().get(i));
      Assert.assertNotEquals(measurement, tablet.getSchemas().get(i).getMeasurementName());
      Assert.assertNotNull(tablet.getValues()[i]);
    }
  }
}
