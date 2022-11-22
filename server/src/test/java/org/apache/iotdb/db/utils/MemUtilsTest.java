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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;

import org.junit.Assert;
import org.junit.Test;

public class MemUtilsTest {

  @Test
  public void getRecordSizeTest() {
    Assert.assertEquals(12, MemUtils.getRecordSize(TSDataType.INT32, 10, true));
    Assert.assertEquals(16, MemUtils.getRecordSize(TSDataType.INT64, 10, true));
    Assert.assertEquals(12, MemUtils.getRecordSize(TSDataType.FLOAT, 10.0, true));
    Assert.assertEquals(16, MemUtils.getRecordSize(TSDataType.DOUBLE, 10.0, true));
    Assert.assertEquals(8, MemUtils.getRecordSize(TSDataType.TEXT, "10", false));
  }

  @Test
  public void getRecordSizeWithInsertNodeTest() throws IllegalPathException {
    PartialPath device = new PartialPath("root.sg.d1");
    String[] measurements = {"s1", "s2", "s3", "s4", "s5"};
    Object[] columns = {
      new int[] {1},
      new long[] {2},
      new float[] {3},
      new double[] {4},
      new Binary[] {new Binary("5")}
    };
    TSDataType[] dataTypes = new TSDataType[6];
    int sizeSum = 0;
    dataTypes[0] = TSDataType.INT32;
    sizeSum += 8 + TSDataType.INT32.getDataTypeSize();
    dataTypes[1] = TSDataType.INT64;
    sizeSum += 8 + TSDataType.INT64.getDataTypeSize();
    dataTypes[2] = TSDataType.FLOAT;
    sizeSum += 8 + TSDataType.FLOAT.getDataTypeSize();
    dataTypes[3] = TSDataType.DOUBLE;
    sizeSum += 8 + TSDataType.DOUBLE.getDataTypeSize();
    dataTypes[4] = TSDataType.TEXT;
    sizeSum += 8 + TSDataType.TEXT.getDataTypeSize();
    InsertTabletNode insertNode =
        new InsertTabletNode(
            new PlanNodeId(""),
            device,
            false,
            measurements,
            dataTypes,
            new long[1],
            null,
            columns,
            1);
    Assert.assertEquals(sizeSum, MemUtils.getTabletSize(insertNode, 0, 1, false));
  }

  /** This method tests MemUtils.getStringMem() and MemUtils.getDataPointMem() */
  @Test
  public void getMemSizeTest() {
    int totalSize = 0;
    String device = "root.sg.d1";
    TSRecord record = new TSRecord(0, device);

    DataPoint point1 = new IntDataPoint("s1", 1);
    Assert.assertEquals(MemUtils.getStringMem("s1") + 20, MemUtils.getDataPointMem(point1));
    totalSize += MemUtils.getDataPointMem(point1);
    record.addTuple(point1);

    DataPoint point2 = new LongDataPoint("s2", 1);
    Assert.assertEquals(MemUtils.getStringMem("s2") + 24, MemUtils.getDataPointMem(point2));
    totalSize += MemUtils.getDataPointMem(point2);
    record.addTuple(point2);

    DataPoint point3 = new FloatDataPoint("s3", 1.0f);
    Assert.assertEquals(MemUtils.getStringMem("s3") + 20, MemUtils.getDataPointMem(point3));
    totalSize += MemUtils.getDataPointMem(point3);
    record.addTuple(point3);

    DataPoint point4 = new DoubleDataPoint("s4", 1.0d);
    Assert.assertEquals(MemUtils.getStringMem("s4") + 24, MemUtils.getDataPointMem(point4));
    totalSize += MemUtils.getDataPointMem(point4);
    record.addTuple(point4);

    DataPoint point5 = new BooleanDataPoint("s5", true);
    Assert.assertEquals(MemUtils.getStringMem("s5") + 17, MemUtils.getDataPointMem(point5));
    totalSize += MemUtils.getDataPointMem(point5);
    record.addTuple(point5);

    DataPoint point6 = new StringDataPoint("s5", Binary.valueOf("123"));
    Assert.assertEquals(MemUtils.getStringMem("s6") + 129, MemUtils.getDataPointMem(point6));
    totalSize += MemUtils.getDataPointMem(point6);
    record.addTuple(point6);

    totalSize += 8 * record.dataPointList.size() + MemUtils.getStringMem(device) + 16;

    Assert.assertEquals(totalSize, MemUtils.getTsRecordMem(record));
  }

  @Test
  public void bytesCntToStrTest() {
    String r = "4 GB 877 MB 539 KB 903 B";
    Assert.assertEquals(r, MemUtils.bytesCntToStr(5215121287L));
  }
}
