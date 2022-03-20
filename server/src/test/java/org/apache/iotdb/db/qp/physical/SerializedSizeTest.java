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
package org.apache.iotdb.db.qp.physical;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class SerializedSizeTest {
  private final String devicePath = "root.test_sg.test_d";

  @Test
  public void testInsertRowPlan() throws IllegalPathException {
    InsertRowPlan insertRowPlan = getInsertRowPlan();
    ByteBuffer buffer = ByteBuffer.allocate(insertRowPlan.serializedSize());
    insertRowPlan.serialize(buffer);
    assertEquals(0, buffer.remaining());
  }

  @Test
  public void testInsertRowsOfOneDevicePlan() throws IllegalPathException {
    InsertRowPlan insertRowPlan1 = getInsertRowPlan();
    InsertRowPlan insertRowPlan2 = getInsertRowPlan();
    insertRowPlan2.setTime(200L);
    InsertRowPlan[] rowPlans = {insertRowPlan1, insertRowPlan2};

    InsertRowsOfOneDevicePlan insertRowsOfOneDevicePlan =
        new InsertRowsOfOneDevicePlan(insertRowPlan1.getDevicePath(), rowPlans, new int[] {0, 1});
    ByteBuffer buffer = ByteBuffer.allocate(insertRowsOfOneDevicePlan.serializedSize());
    insertRowsOfOneDevicePlan.serialize(buffer);
    assertEquals(0, buffer.remaining());
  }

  @Test
  public void testInsertRowsPlan() throws IllegalPathException {
    InsertRowPlan insertRowPlan1 = getInsertRowPlan();
    InsertRowPlan insertRowPlan2 = getInsertRowPlan();
    insertRowPlan2.setTime(200L);

    InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
    insertRowsPlan.addOneInsertRowPlan(insertRowPlan1, 0);
    insertRowsPlan.addOneInsertRowPlan(insertRowPlan2, 1);
    ByteBuffer buffer = ByteBuffer.allocate(insertRowsPlan.serializedSize());
    insertRowsPlan.serialize(buffer);
    assertEquals(0, buffer.remaining());
  }

  private InsertRowPlan getInsertRowPlan() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    String[] columns = new String[6];
    columns[0] = 1.0 + "";
    columns[1] = 2 + "";
    columns[2] = 10000 + "";
    columns[3] = 100 + "";
    columns[4] = false + "";
    columns[5] = "hh" + 0;

    return new InsertRowPlan(
        new PartialPath(devicePath),
        time,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
        dataTypes,
        columns);
  }

  @Test
  public void testInsertTabletPlan() throws IllegalPathException {
    InsertTabletPlan insertTabletPlan = getInsertTabletPlan();
    ByteBuffer buffer = ByteBuffer.allocate(insertTabletPlan.serializedSize());
    insertTabletPlan.serialize(buffer);
    assertEquals(0, buffer.remaining());
  }

  @Test
  public void testInsertMultiTabletPlan() throws IllegalPathException {
    List<InsertTabletPlan> insertTabletPlans = new ArrayList<>();
    InsertTabletPlan insertTabletPlan1 = getInsertTabletPlan();
    insertTabletPlans.add(insertTabletPlan1);
    InsertTabletPlan insertTabletPlan2 = getInsertTabletPlan();
    insertTabletPlan2.setTimes(new long[] {114L, 115L, 116L, 117L});
    insertTabletPlans.add(insertTabletPlan2);

    InsertMultiTabletPlan insertMultiTabletPlan = new InsertMultiTabletPlan(insertTabletPlans);
    ByteBuffer buffer = ByteBuffer.allocate(insertMultiTabletPlan.serializedSize());
    insertMultiTabletPlan.serialize(buffer);
    assertEquals(0, buffer.remaining());
  }

  private InsertTabletPlan getInsertTabletPlan() throws IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());

    Object[] columns = new Object[6];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];
    columns[5] = new Binary[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0 + r;
      ((float[]) columns[1])[r] = 2 + r;
      ((long[]) columns[2])[r] = 10000 + r;
      ((int[]) columns[3])[r] = 100 + r;
      ((boolean[]) columns[4])[r] = (r % 2 == 0);
      ((Binary[]) columns[5])[r] = new Binary("hh" + r);
    }

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(
            new PartialPath(devicePath),
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes);
    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);
    insertTabletPlan.setBitMaps(bitMaps);
    return insertTabletPlan;
  }
}
