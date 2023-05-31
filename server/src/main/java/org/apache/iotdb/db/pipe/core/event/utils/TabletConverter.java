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

package org.apache.iotdb.db.pipe.core.event.utils;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import javax.activation.UnsupportedDataTypeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TabletConverter {
  private TabletConverter() {
    // private constructor to prevent instantiation
  }

  public static Tablet toTablet(InsertNode insertNode) throws UnsupportedDataTypeException {
    long[] timestamps;
    List<TSDataType> dataTypes = Arrays.asList(insertNode.getDataTypes());
    List<Path> columnPaths = new ArrayList<>();
    String deviceId = insertNode.getDevicePath().getFullPath();
    int columnCount = insertNode.getMeasurements().length;
    Object[] columns;
    BitMap[] bitMaps;
    int rowCount;

    for (String measurement : insertNode.getMeasurements()) {
      columnPaths.add(new Path(deviceId, measurement, false));
    }

    if (insertNode instanceof InsertRowNode) {
      InsertRowNode rowNode = (InsertRowNode) insertNode;
      bitMaps = new BitMap[columnCount];
      Object[] values = rowNode.getValues();
      columns = new Object[columnCount];
      for (int i = 0; i < columnCount; i++) {
        columns[i] = values[i];
        bitMaps[i] = new BitMap(1);
        if (values[i] == null) {
          bitMaps[i].mark(0);
        }
      }
      timestamps = new long[] {rowNode.getTime()};
      rowCount = 1;
    } else if (insertNode instanceof InsertTabletNode) {
      InsertTabletNode tabletNode = (InsertTabletNode) insertNode;
      rowCount = tabletNode.getRowCount();
      timestamps = tabletNode.getTimes();
      bitMaps = tabletNode.getBitMaps();
      columns = tabletNode.getColumns();
    } else {
      throw new UnsupportedDataTypeException(
          String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
    }

    List<MeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < columnPaths.size(); i++) {
      schemas.add(new MeasurementSchema(columnPaths.get(i).getMeasurement(), dataTypes.get(i)));
    }

    return new Tablet(deviceId, schemas, timestamps, columns, bitMaps, rowCount);
  }
}
