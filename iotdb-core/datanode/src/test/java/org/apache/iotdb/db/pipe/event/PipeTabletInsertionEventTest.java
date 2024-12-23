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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventTreePatternParser;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;

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
import java.util.Arrays;

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
    BitMap[] bitMapsForInsertRowNode = new BitMap[schemas.length];
    for (int i = 0; i < schemas.length; i++) {
      bitMapsForInsertRowNode[i] = new BitMap(1);
    }

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

    tabletForInsertRowNode = new Tablet(deviceId, Arrays.asList(schemas), 1);
    tabletForInsertRowNode.values = values;
    tabletForInsertRowNode.timestamps = new long[] {times[0]};
    tabletForInsertRowNode.setRowSize(1);
    tabletForInsertRowNode.bitMaps = bitMapsForInsertRowNode;

    // create tablet for insertTabletNode
    BitMap[] bitMapsForInsertTabletNode = new BitMap[schemas.length];
    for (int i = 0; i < schemas.length; i++) {
      bitMapsForInsertTabletNode[i] = new BitMap(times.length);
    }

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
    tabletForInsertTabletNode.values = values;
    tabletForInsertTabletNode.timestamps = times;
    tabletForInsertTabletNode.setRowSize(times.length);
    tabletForInsertTabletNode.bitMaps = bitMapsForInsertTabletNode;
  }

  @Test
  public void convertToTabletForTest() {
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
  public void convertToAlignedTabletForTest() {
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
  public void convertToTabletWithFilteredRowsForTest() {
    TabletInsertionEventTreePatternParser container1 =
        new TabletInsertionEventTreePatternParser(
            null,
            new PipeRawTabletInsertionEvent(tabletForInsertRowNode, 111L, 113L),
            insertRowNode,
            new PrefixTreePattern(pattern));
    Tablet tablet1 = container1.convertToTablet();
    Assert.assertEquals(0, tablet1.getRowSize());
    boolean isAligned1 = container1.isAligned();
    Assert.assertFalse(isAligned1);

    TabletInsertionEventTreePatternParser container2 =
        new TabletInsertionEventTreePatternParser(
            null,
            new PipeRawTabletInsertionEvent(tabletForInsertRowNode, 110L, 110L),
            insertRowNode,
            new PrefixTreePattern(pattern));
    Tablet tablet2 = container2.convertToTablet();
    Assert.assertEquals(1, tablet2.getRowSize());
    boolean isAligned2 = container2.isAligned();
    Assert.assertFalse(isAligned2);

    TabletInsertionEventTreePatternParser container3 =
        new TabletInsertionEventTreePatternParser(
            null,
            new PipeRawTabletInsertionEvent(tabletForInsertTabletNode, 111L, 113L),
            insertTabletNode,
            new PrefixTreePattern(pattern));
    Tablet tablet3 = container3.convertToTablet();
    Assert.assertEquals(3, tablet3.getRowSize());
    boolean isAligned3 = container3.isAligned();
    Assert.assertFalse(isAligned3);

    TabletInsertionEventTreePatternParser container4 =
        new TabletInsertionEventTreePatternParser(
            null,
            new PipeRawTabletInsertionEvent(tabletForInsertTabletNode, Long.MIN_VALUE, 109L),
            insertTabletNode,
            new PrefixTreePattern(pattern));
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
}
