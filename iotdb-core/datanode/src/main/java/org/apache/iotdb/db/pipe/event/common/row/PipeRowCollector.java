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
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletEventConverter;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils;
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

public class PipeRowCollector extends PipeRawTabletEventConverter implements RowCollector {
  private Tablet tablet = null;

  public PipeRowCollector(PipeTaskMeta pipeTaskMeta, EnrichedEvent sourceEvent) {
    super(pipeTaskMeta, sourceEvent);
  }

  public PipeRowCollector(
      PipeTaskMeta pipeTaskMeta,
      EnrichedEvent sourceEvent,
      String sourceEventDataBase,
      Boolean isTableModel) {
    super(pipeTaskMeta, sourceEvent, sourceEventDataBase, isTableModel);
  }

  public PipeRowCollector(
      PipeTaskMeta pipeTaskMeta,
      EnrichedEvent sourceEvent,
      String sourceEventDataBase,
      Boolean isTableModel,
      String rawTableModelDataBaseName,
      String rawTreeModelDataBaseName) {
    super(
        pipeTaskMeta,
        sourceEvent,
        sourceEventDataBase,
        isTableModel,
        rawTableModelDataBaseName,
        rawTreeModelDataBaseName);
  }

  @Override
  public void collectRow(Row row) {
    if (!(row instanceof PipeRow)) {
      throw new PipeException(DataNodePipeMessages.ROW_CAN_NOT_BE_CUSTOMIZED);
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
      isAligned = pipeRow.isAligned();
    }

    final int rowIndex = tablet.getRowSize();
    tablet.addTimestamp(rowIndex, row.getTime());
    for (int i = 0; i < row.size(); i++) {
      final Object value = row.getObject(i);
      PipeTabletUtils.putValue(
          tablet,
          rowIndex,
          i,
          measurementSchemaArray[i].getType(),
          value instanceof Binary
              ? PipeBinaryTransformer.transformToBinary((Binary) value)
              : value);
      if (row.isNull(i)) {
        PipeTabletUtils.markNullValue(tablet, rowIndex, i);
      }
    }

    if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
      collectTabletInsertionEvent();
    }
  }

  private void collectTabletInsertionEvent() {
    if (tablet != null) {
      PipeTabletUtils.compactBitMaps(tablet);
      tabletInsertionEventList.add(
          new PipeRawTabletInsertionEvent(
              isTableModel,
              sourceEventDataBaseName,
              rawTableModelDataBaseName,
              rawTreeModelDataBaseName,
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

  @Override
  public List<TabletInsertionEvent> convertToTabletInsertionEvents(final boolean shouldReport) {
    collectTabletInsertionEvent();
    return super.convertToTabletInsertionEvents(shouldReport);
  }
}
