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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.TabletInsertionDataContainer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class PipeTabletInsertionEventTest {

  private InsertRowNode insertRowNode;
  private InsertRowNode insertRowNodeAligned;
  private InsertTabletNode insertTabletNode;
  private InsertTabletNode insertTabletNodeAligned;

  final String deviceId = "root.sg.d1";
  final long[] times = new long[] {110L, 111L, 112L, 113L, 114L};
  final String[] measurementIds = new String[] {"s1", "s2", "s3", "s4", "s5", "s6"};
  final TSDataType[] dataTypes =
      new TSDataType[] {
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.BOOLEAN,
        TSDataType.TEXT
      };

  final MeasurementSchema[] schemas = new MeasurementSchema[6];

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
    for (int i = 0; i < 6; i++) {
      schemas[i] = new MeasurementSchema(measurementIds[i], dataTypes[i]);
    }
  }

  private void createInsertRowNode() throws IllegalPathException {
    final Object[] values = new Object[6];

    values[0] = 100;
    values[1] = 10000L;
    values[2] = 2F;
    values[3] = 1.0;
    values[4] = false;
    values[5] = Binary.valueOf("text");

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
    final Object[] values = new Object[6];

    values[0] = new int[5];
    values[1] = new long[5];
    values[2] = new float[5];
    values[3] = new double[5];
    values[4] = new boolean[5];
    values[5] = new Binary[5];

    for (int r = 0; r < 5; r++) {
      ((int[]) values[0])[r] = 100;
      ((long[]) values[1])[r] = 10000;
      ((float[]) values[2])[r] = 2;
      ((double[]) values[3])[r] = 1.0;
      ((boolean[]) values[4])[r] = false;
      ((Binary[]) values[5])[r] = Binary.valueOf("text");
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
    final Object[] values = new Object[6];

    // create tablet for insertRowNode
    BitMap[] bitMapsForInsertRowNode = new BitMap[6];
    for (int i = 0; i < 6; i++) {
      bitMapsForInsertRowNode[i] = new BitMap(1);
    }

    values[0] = new int[1];
    values[1] = new long[1];
    values[2] = new float[1];
    values[3] = new double[1];
    values[4] = new boolean[1];
    values[5] = new Binary[1];

    for (int r = 0; r < 1; r++) {
      ((int[]) values[0])[r] = 100;
      ((long[]) values[1])[r] = 10000;
      ((float[]) values[2])[r] = 2;
      ((double[]) values[3])[r] = 1.0;
      ((boolean[]) values[4])[r] = false;
      ((Binary[]) values[5])[r] = Binary.valueOf("text");
    }

    tabletForInsertRowNode = new Tablet(deviceId, Arrays.asList(schemas), 1);
    tabletForInsertRowNode.values = values;
    tabletForInsertRowNode.timestamps = new long[] {times[0]};
    tabletForInsertRowNode.rowSize = 1;
    tabletForInsertRowNode.bitMaps = bitMapsForInsertRowNode;

    // create tablet for insertTabletNode
    BitMap[] bitMapsForInsertTabletNode = new BitMap[6];
    for (int i = 0; i < 6; i++) {
      bitMapsForInsertTabletNode[i] = new BitMap(times.length);
    }

    values[0] = new int[5];
    values[1] = new long[5];
    values[2] = new float[5];
    values[3] = new double[5];
    values[4] = new boolean[5];
    values[5] = new Binary[5];

    for (int r = 0; r < 5; r++) {
      ((int[]) values[0])[r] = 100;
      ((long[]) values[1])[r] = 10000;
      ((float[]) values[2])[r] = 2;
      ((double[]) values[3])[r] = 1.0;
      ((boolean[]) values[4])[r] = false;
      ((Binary[]) values[5])[r] = Binary.valueOf("text");
    }
    tabletForInsertTabletNode = new Tablet(deviceId, Arrays.asList(schemas), times.length);
    tabletForInsertTabletNode.values = values;
    tabletForInsertTabletNode.timestamps = times;
    tabletForInsertTabletNode.rowSize = times.length;
    tabletForInsertTabletNode.bitMaps = bitMapsForInsertTabletNode;
  }

  @Test
  public void convertToTabletForTest() {
    TabletInsertionDataContainer container1 =
        new TabletInsertionDataContainer(insertRowNode, pattern);
    Tablet tablet1 = container1.convertToTablet();
    boolean isAligned1 = container1.isAligned();
    Assert.assertEquals(tablet1, tabletForInsertRowNode);
    Assert.assertFalse(isAligned1);

    TabletInsertionDataContainer container2 =
        new TabletInsertionDataContainer(insertTabletNode, pattern);
    Tablet tablet2 = container2.convertToTablet();
    boolean isAligned2 = container2.isAligned();
    Assert.assertEquals(tablet2, tabletForInsertTabletNode);
    Assert.assertFalse(isAligned2);

    PipeRawTabletInsertionEvent event3 = new PipeRawTabletInsertionEvent(tablet1, pattern);
    Tablet tablet3 = event3.convertToTablet();
    boolean isAligned3 = event3.isAligned();
    Assert.assertEquals(tablet1, tablet3);
    Assert.assertFalse(isAligned3);

    PipeRawTabletInsertionEvent event4 = new PipeRawTabletInsertionEvent(tablet2, pattern);
    Tablet tablet4 = event4.convertToTablet();
    boolean isAligned4 = event4.isAligned();
    Assert.assertEquals(tablet2, tablet4);
    Assert.assertFalse(isAligned4);
  }

  @Test
  public void convertToAlignedTabletForTest() {
    TabletInsertionDataContainer container1 =
        new TabletInsertionDataContainer(insertRowNodeAligned, pattern);
    Tablet tablet1 = container1.convertToTablet();
    boolean isAligned1 = container1.isAligned();
    Assert.assertEquals(tablet1, tabletForInsertRowNode);
    Assert.assertTrue(isAligned1);

    TabletInsertionDataContainer container2 =
        new TabletInsertionDataContainer(insertTabletNodeAligned, pattern);
    Tablet tablet2 = container2.convertToTablet();
    boolean isAligned2 = container2.isAligned();
    Assert.assertEquals(tablet2, tabletForInsertTabletNode);
    Assert.assertTrue(isAligned2);

    PipeRawTabletInsertionEvent event3 = new PipeRawTabletInsertionEvent(tablet1, true, pattern);
    Tablet tablet3 = event3.convertToTablet();
    boolean isAligned3 = event3.isAligned();
    Assert.assertEquals(tablet1, tablet3);
    Assert.assertTrue(isAligned3);

    PipeRawTabletInsertionEvent event4 = new PipeRawTabletInsertionEvent(tablet2, true, pattern);
    Tablet tablet4 = event4.convertToTablet();
    boolean isAligned4 = event4.isAligned();
    Assert.assertEquals(tablet2, tablet4);
    Assert.assertTrue(isAligned4);
  }
}
