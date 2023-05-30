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

package org.apache.iotdb.db.pipe.core.event.view.collector;

import org.apache.iotdb.commons.pipe.utils.PipeDataTypeTransformer;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTabletInsertionEvent;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.type.Type;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PipeRowCollector implements RowCollector {

  private Tablet tablet = null;

  @Override
  public void collectRow(Row row) throws IOException {
    List<Path> measurementIds = row.getColumnNames();
    List<Type> dataTypeList = row.getColumnTypes();

    if (tablet == null) {
      String deviceId = measurementIds.get(0).getDevice();
      List<MeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < row.size(); i++) {
        schemaList.add(
            new MeasurementSchema(
                measurementIds.get(i).getMeasurement(),
                PipeDataTypeTransformer.transformToTsDataType(dataTypeList.get(i))));
      }
      tablet = new Tablet(deviceId, schemaList);
    }

    int rowIndex = (tablet.rowSize++) - 1;
    tablet.addTimestamp(rowIndex, row.getTime());
    for (int i = 0; i < row.size(); i++) {
      tablet.addValue(measurementIds.get(i).getMeasurement(), rowIndex, row.getObject(i));
      if (row.getObject(i) == null) {
        tablet.bitMaps[i].mark(rowIndex);
      }
    }
  }

  public TabletInsertionEvent toTabletInsertionEvent() {
    PipeTabletInsertionEvent tabletInsertionEvent = new PipeTabletInsertionEvent(tablet);
    this.tablet = null;
    return tabletInsertionEvent;
  }
}
