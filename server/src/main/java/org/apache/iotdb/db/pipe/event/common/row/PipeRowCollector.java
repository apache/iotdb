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

package org.apache.iotdb.db.pipe.event.common.row;

import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PipeRowCollector implements RowCollector {

  private final List<TabletInsertionEvent> tabletInsertionEventList = new ArrayList<>();
  private Tablet tablet = null;

  @Override
  public void collectRow(Row row) {
    if (!(row instanceof PipeRow)) {
      throw new PipeException("Row can not be customized");
    }

    final PipeRow pipeRow = (PipeRow) row;
    final MeasurementSchema[] measurementSchemaArray = pipeRow.getMeasurementSchemaList();

    if (tablet == null) {
      final String deviceId = pipeRow.getDeviceId();
      final List<MeasurementSchema> measurementSchemaList =
          new ArrayList<>(Arrays.asList(measurementSchemaArray));
      tablet = new Tablet(deviceId, measurementSchemaList);
      tablet.initBitMaps();
    }

    final int rowIndex = tablet.rowSize;
    tablet.addTimestamp(rowIndex, row.getTime());
    for (int i = 0; i < row.size(); i++) {
      tablet.addValue(measurementSchemaArray[i].getMeasurementId(), rowIndex, row.getObject(i));
      if (row.isNull(i)) {
        tablet.bitMaps[i].mark(rowIndex);
      }
    }
    tablet.rowSize++;

    if (tablet.rowSize == tablet.getMaxRowNumber()) {
      collectTabletInsertionEvent();
    }
  }

  private void collectTabletInsertionEvent() {
    if (tablet != null) {
      tabletInsertionEventList.add(new PipeRawTabletInsertionEvent(tablet));
    }
    this.tablet = null;
  }

  public Iterable<TabletInsertionEvent> convertToTabletInsertionEvents() {
    collectTabletInsertionEvent();
    return tabletInsertionEventList;
  }
}
