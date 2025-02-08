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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
  public void getRecordSizeWithInsertRowNodeTest() {
    Object[] row = {1, 2L, 3.0f, 4.0d, new Binary("5", TSFileConfig.STRING_CHARSET)};
    List<TSDataType> dataTypes = new ArrayList<>();
    int sizeSum = 0;
    dataTypes.add(TSDataType.INT32);
    sizeSum += 8 + TSDataType.INT32.getDataTypeSize();
    dataTypes.add(TSDataType.INT64);
    sizeSum += 8 + TSDataType.INT64.getDataTypeSize();
    dataTypes.add(TSDataType.FLOAT);
    sizeSum += 8 + TSDataType.FLOAT.getDataTypeSize();
    dataTypes.add(TSDataType.DOUBLE);
    sizeSum += 8 + TSDataType.DOUBLE.getDataTypeSize();
    dataTypes.add(TSDataType.TEXT);
    sizeSum += 8;
    Assert.assertEquals(sizeSum, MemUtils.getRowRecordSize(dataTypes, row));
  }

  @Test
  public void getRecordSizeWithInsertAlignedRowNodeTest() {
    Object[] row = {1, 2L, 3.0f, 4.0d, new Binary("5", TSFileConfig.STRING_CHARSET)};
    List<TSDataType> dataTypes = new ArrayList<>();
    int sizeSum = 0;
    dataTypes.add(TSDataType.INT32);
    sizeSum += TSDataType.INT32.getDataTypeSize();
    dataTypes.add(TSDataType.INT64);
    sizeSum += TSDataType.INT64.getDataTypeSize();
    dataTypes.add(TSDataType.FLOAT);
    sizeSum += TSDataType.FLOAT.getDataTypeSize();
    dataTypes.add(TSDataType.DOUBLE);
    sizeSum += TSDataType.DOUBLE.getDataTypeSize();
    dataTypes.add(TSDataType.TEXT);
    // time and index size
    sizeSum += 8 + 4;
    Assert.assertEquals(sizeSum, MemUtils.getAlignedRowRecordSize(dataTypes, row, null));
  }

  @Test
  public void getRecordSizeWithInsertTableNodeTest() throws IllegalPathException {
    PartialPath device = new PartialPath("root.sg.d1");
    String[] measurements = {"s1", "s2", "s3", "s4", "s5"};
    Object[] columns = {
      new int[] {1},
      new long[] {2},
      new float[] {3},
      new double[] {4},
      new Binary[] {new Binary("5", TSFileConfig.STRING_CHARSET)}
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
    Assert.assertEquals(sizeSum, MemUtils.getTabletSize(insertNode, 0, 1));
  }

  @Test
  public void getRecordSizeWithInsertAlignedTableNodeTest() throws IllegalPathException {
    PartialPath device = new PartialPath("root.sg.d1");
    String[] measurements = {"s1", "s2", "s3", "s4", "s5"};
    Object[] columns = {
      new int[] {1},
      new long[] {2},
      new float[] {3},
      new double[] {4},
      new Binary[] {new Binary("5", TSFileConfig.STRING_CHARSET)}
    };
    TSDataType[] dataTypes = new TSDataType[6];
    int sizeSum = 0;
    dataTypes[0] = TSDataType.INT32;
    sizeSum += TSDataType.INT32.getDataTypeSize();
    dataTypes[1] = TSDataType.INT64;
    sizeSum += TSDataType.INT64.getDataTypeSize();
    dataTypes[2] = TSDataType.FLOAT;
    sizeSum += TSDataType.FLOAT.getDataTypeSize();
    dataTypes[3] = TSDataType.DOUBLE;
    sizeSum += TSDataType.DOUBLE.getDataTypeSize();
    dataTypes[4] = TSDataType.TEXT;
    sizeSum += TSDataType.TEXT.getDataTypeSize();
    // time and index size
    sizeSum += 8 + 4;
    InsertTabletNode insertNode =
        new InsertTabletNode(
            new PlanNodeId(""),
            device,
            true,
            measurements,
            dataTypes,
            new long[1],
            null,
            columns,
            1);
    insertNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64),
          new MeasurementSchema("s3", TSDataType.FLOAT),
          new MeasurementSchema("s4", TSDataType.DOUBLE),
          new MeasurementSchema("s5", TSDataType.TEXT)
        });
    Assert.assertEquals(sizeSum, MemUtils.getAlignedTabletSize(insertNode, 0, 1, null));
  }

  /** This method tests MemUtils.getStringMem() and MemUtils.getDataPointMem() */
  @Test
  public void getMemSizeTest() {
    long totalSize = 0;
    IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1");
    TSRecord record = new TSRecord(device, 0);

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

    DataPoint point6 = new StringDataPoint("s5", BytesUtils.valueOf("123"));
    Assert.assertEquals(MemUtils.getStringMem("s6") + 129, MemUtils.getDataPointMem(point6));
    totalSize += MemUtils.getDataPointMem(point6);
    record.addTuple(point6);

    totalSize += 8L * record.dataPointList.size() + device.ramBytesUsed() + 16;

    Assert.assertEquals(totalSize, MemUtils.getTsRecordMem(record));
  }

  @Test
  public void bytesCntToStrTest() {
    String r = "4 GB 877 MB 539 KB 903 B";
    Assert.assertEquals(r, MemUtils.bytesCntToStr(5215121287L));
  }
}
