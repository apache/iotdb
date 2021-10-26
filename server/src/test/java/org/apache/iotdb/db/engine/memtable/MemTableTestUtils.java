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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class MemTableTestUtils {

  public static String deviceId0 = "d0";

  public static String measurementId0 = "s0";

  public static TSDataType dataType0 = TSDataType.INT32;
  private static Schema schema = new Schema();

  static {
    schema.registerTimeseries(
        new Path(deviceId0, measurementId0),
        new UnaryMeasurementSchema(measurementId0, dataType0, TSEncoding.PLAIN));
  }

  public static void produceData(
      IMemTable iMemTable,
      long startTime,
      long endTime,
      String deviceId,
      String measurementId,
      TSDataType dataType) {
    if (startTime > endTime) {
      throw new RuntimeException(String.format("start time %d > end time %d", startTime, endTime));
    }
    for (long l = startTime; l <= endTime; l++) {
      iMemTable.write(
          deviceId,
          new UnaryMeasurementSchema(measurementId, dataType, TSEncoding.PLAIN),
          l,
          (int) l);
    }
  }

  public static void produceVectorData(IMemTable iMemTable) throws IllegalPathException {
    iMemTable.write(genInsertTablePlan(), 1, 101);
  }

  private static InsertTabletPlan genInsertTablePlan() throws IllegalPathException {
    String[] measurements = new String[2];
    measurements[0] = "sensor0";
    measurements[1] = "sensor1";

    List<Integer> dataTypesList = new ArrayList<>();
    TSDataType[] dataTypes = new TSDataType[2];
    dataTypesList.add(TSDataType.BOOLEAN.ordinal());
    dataTypesList.add(TSDataType.INT64.ordinal());
    dataTypes[0] = TSDataType.BOOLEAN;
    dataTypes[1] = TSDataType.INT64;

    TSEncoding[] encodings = new TSEncoding[2];
    encodings[0] = TSEncoding.PLAIN;
    encodings[1] = TSEncoding.GORILLA;

    IMeasurementMNode[] mNodes = new IMeasurementMNode[2];
    IMeasurementSchema schema =
        new VectorMeasurementSchema("vectorName", measurements, dataTypes, encodings);
    mNodes[0] = MeasurementMNode.getMeasurementMNode(null, "sensor0", schema, null);
    mNodes[1] = MeasurementMNode.getMeasurementMNode(null, "sensor1", schema, null);

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(
            new PartialPath(deviceId0), new String[] {"(sensor0,sensor1)"}, dataTypesList);

    insertTabletPlan.setAligned(true);

    long[] times = new long[101];
    Object[] columns = new Object[2];
    columns[0] = new boolean[101];
    columns[1] = new long[101];

    for (long r = 0; r < 101; r++) {
      times[(int) r] = r;
      ((boolean[]) columns[0])[(int) r] = false;
      ((long[]) columns[1])[(int) r] = r;
    }
    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);
    insertTabletPlan.setMeasurementMNodes(mNodes);
    insertTabletPlan.setStart(0);
    insertTabletPlan.setEnd(100);

    return insertTabletPlan;
  }

  public static void produceNullableVectorData(IMemTable iMemTable) throws IllegalPathException {
    InsertTabletPlan plan = genInsertTablePlan();
    BitMap[] bitMaps = new BitMap[2];
    bitMaps[1] = new BitMap(101);
    for (int r = 0; r < 101; r++) {
      if (r % 2 == 1) {
        bitMaps[1].mark(r);
      }
    }
    plan.setBitMaps(bitMaps);
    iMemTable.write(plan, 1, 101);
  }

  public static Schema getSchema() {
    return schema;
  }
}
