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

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.type.Binary;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PipeRowCollector implements RowCollector {

  private final List<TabletInsertionEvent> tabletInsertionEventList = new ArrayList<>();
  private Tablet tablet = null;
  private boolean isAligned = false;
  private final PipeTaskMeta pipeTaskMeta; // Used to report progress
  private final EnrichedEvent sourceEvent; // Used to report progress

  public PipeRowCollector(PipeTaskMeta pipeTaskMeta, EnrichedEvent sourceEvent) {
    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceEvent = sourceEvent;
  }

  @Override
  public void collectRow(Row row) {
    if (!(row instanceof PipeRow)) {
      throw new PipeException("Row can not be customized");
    }

    final PipeRow pipeRow = (PipeRow) row;
    final IMeasurementSchema[] measurementSchemaArray = pipeRow.getMeasurementSchemaList();

    // Trigger collection when a PipeResetTabletRow is encountered
    if (row instanceof PipeResetTabletRow) {
      collectTabletInsertionEvent();
    }

    if (tablet == null) {
      final String deviceId = pipeRow.getDeviceId();
      final List<IMeasurementSchema> measurementSchemaList =
          new ArrayList<>(Arrays.asList(measurementSchemaArray));
      // Calculate row count and memory size of the tablet based on the first row
      Pair<Integer, Integer> rowCountAndMemorySize =
          PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(pipeRow);
      tablet = new Tablet(deviceId, measurementSchemaList, rowCountAndMemorySize.getLeft());
      tablet.initBitMaps();
      isAligned = pipeRow.isAligned();
    }

    final int rowIndex = tablet.getRowSize();
    tablet.addTimestamp(rowIndex, row.getTime());
    for (int i = 0; i < row.size(); i++) {
      final Object value = row.getObject(i);
      if (value instanceof Binary) {
        tablet.addValue(
            measurementSchemaArray[i].getMeasurementName(),
            rowIndex,
            PipeBinaryTransformer.transformToBinary((Binary) value));
      } else {
        tablet.addValue(measurementSchemaArray[i].getMeasurementName(), rowIndex, value);
      }
      if (row.isNull(i)) {
        tablet.bitMaps[i].mark(rowIndex);
      }
    }

    if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
      collectTabletInsertionEvent();
    }
  }

  private void collectTabletInsertionEvent() {
    if (tablet != null) {
      // TODO: non-PipeInsertionEvent sourceEvent is not supported?
      final PipeInsertionEvent pipeInsertionEvent =
          sourceEvent instanceof PipeInsertionEvent ? ((PipeInsertionEvent) sourceEvent) : null;
      tabletInsertionEventList.add(
          new PipeRawTabletInsertionEvent(
              pipeInsertionEvent == null ? null : pipeInsertionEvent.isTableModelEvent(),
              pipeInsertionEvent == null ? null : pipeInsertionEvent.getTreeModelDatabaseName(),
              tablet,
              isAligned,
              sourceEvent == null ? null : sourceEvent.getPipeName(),
              sourceEvent == null ? 0 : sourceEvent.getCreationTime(),
              pipeTaskMeta,
              sourceEvent,
              false));
    }
    this.tablet = null;
  }

  public List<TabletInsertionEvent> convertToTabletInsertionEvents(final boolean shouldReport) {
    collectTabletInsertionEvent();

    final int eventListSize = tabletInsertionEventList.size();
    if (eventListSize > 0 && shouldReport) { // The last event should report progress
      ((PipeRawTabletInsertionEvent) tabletInsertionEventList.get(eventListSize - 1))
          .markAsNeedToReport();
    }
    return tabletInsertionEventList;
  }
}
