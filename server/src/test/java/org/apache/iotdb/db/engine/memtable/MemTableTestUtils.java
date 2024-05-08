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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import java.util.Collections;

public class MemTableTestUtils {

  public static String deviceId0 = "d0";

  public static String measurementId0 = "s0";

  public static TSDataType dataType0 = TSDataType.INT32;
  private static Schema schema = new Schema();

  static {
    schema.registerTimeseries(
        new Path(deviceId0), new MeasurementSchema(measurementId0, dataType0, TSEncoding.PLAIN));
  }

  public static void produceData(
      IMemTable iMemTable,
      long startTime,
      long endTime,
      String deviceId,
      String measurementId,
      TSDataType dataType)
      throws IllegalPathException {
    if (startTime > endTime) {
      throw new RuntimeException(String.format("start time %d > end time %d", startTime, endTime));
    }
    for (long l = startTime; l <= endTime; l++) {
      iMemTable.write(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId, dataType, TSEncoding.PLAIN)),
          l,
          new Object[] {(int) l});
    }
  }

  public static void produceVectorData(IMemTable iMemTable)
      throws IllegalPathException, WriteProcessException {
    iMemTable.insertTablet(genInsertTableNode(), 1, 101);
  }

  private static InsertTabletNode genInsertTableNode() throws IllegalPathException {
    String[] measurements = new String[2];
    measurements[0] = "sensor0";
    measurements[1] = "sensor1";
    TSDataType[] dataTypes = new TSDataType[2];
    dataTypes[0] = TSDataType.BOOLEAN;
    dataTypes[1] = TSDataType.INT64;
    long[] times = new long[101];
    Object[] columns = new Object[2];
    columns[0] = new boolean[101];
    columns[1] = new long[101];

    for (long r = 0; r < 101; r++) {
      times[(int) r] = r;
      ((boolean[]) columns[0])[(int) r] = false;
      ((long[]) columns[1])[(int) r] = r;
    }
    TSEncoding[] encodings = new TSEncoding[2];
    encodings[0] = TSEncoding.PLAIN;
    encodings[1] = TSEncoding.GORILLA;

    MeasurementSchema[] schemas = new MeasurementSchema[2];
    schemas[0] = new MeasurementSchema(measurements[0], dataTypes[0], encodings[0]);
    schemas[1] = new MeasurementSchema(measurements[1], dataTypes[1], encodings[1]);
    InsertTabletNode node =
        new InsertTabletNode(
            new PlanNodeId("0"),
            new PartialPath(deviceId0),
            true,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    node.setMeasurementSchemas(schemas);
    return node;
  }

  public static void produceNullableVectorData(IMemTable iMemTable)
      throws IllegalPathException, WriteProcessException {
    InsertTabletNode node = genInsertTableNode();
    BitMap[] bitMaps = new BitMap[2];
    bitMaps[1] = new BitMap(101);
    for (int r = 0; r < 101; r++) {
      if (r % 2 == 1) {
        bitMaps[1].mark(r);
      }
    }
    node.setBitMaps(bitMaps);
    iMemTable.insertTablet(node, 1, 101);
  }

  public static Schema getSchema() {
    return schema;
  }
}
