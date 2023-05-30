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
  private final InsertNode insertNode;
  private long[] timestamps;
  private List<TSDataType> dataTypes;
  private final List<Path> columnPaths;
  private String deviceId;
  private Object[] columns;
  private BitMap[] bitMaps;
  private int rowSize;

  public TabletConverter(InsertNode insertNode) {
    this.insertNode = insertNode;
    this.columnPaths = new ArrayList<>();
  }

  public Tablet toTablet() throws UnsupportedDataTypeException {
    if (insertNode instanceof InsertRowNode) {
      return handleRowNode((InsertRowNode) insertNode);
    } else if (insertNode instanceof InsertTabletNode) {
      return handleTabletNode((InsertTabletNode) insertNode);
    } else {
      throw new UnsupportedDataTypeException(
          String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
    }
  }

  private Tablet handleRowNode(InsertRowNode rowNode) {
    setupMetadata(rowNode);
    columns[0] = rowNode.getValues();
    this.timestamps = new long[] {rowNode.getTime()};
    return createTablet();
  }

  private Tablet handleTabletNode(InsertTabletNode tabletNode) {
    setupMetadata(tabletNode);
    this.rowSize = tabletNode.getRowCount();
    this.timestamps = tabletNode.getTimes();
    this.bitMaps = tabletNode.getBitMaps();
    this.columns = tabletNode.getColumns();
    return createTablet();
  }

  private List<MeasurementSchema> getMeasurementSchemas() {
    List<MeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < columnPaths.size(); i++) {
      schemas.add(new MeasurementSchema(columnPaths.get(i).getMeasurement(), dataTypes.get(i)));
    }
    return schemas;
  }

  private void setupMetadata(InsertNode node) {
    this.dataTypes = Arrays.asList(node.getDataTypes());
    this.deviceId = node.getDevicePath().getFullPath();

    for (String measurement : node.getMeasurements()) {
      this.columnPaths.add(new Path(deviceId, measurement, false));
    }
  }

  private Tablet createTablet() {
    List<MeasurementSchema> schemas = getMeasurementSchemas();
    return new Tablet(deviceId, schemas, timestamps, columns, bitMaps, rowSize);
  }
}
