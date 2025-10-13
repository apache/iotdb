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

package org.apache.iotdb.db.pipe.event.common.tablet.parser;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.pipe.event.common.row.PipeRow;
import org.apache.iotdb.db.pipe.event.common.row.PipeRowCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

public class TabletInsertionEventTreePatternParser extends TabletInsertionEventParser {

  private final TreePattern pattern;

  // Exclusion patterns for tree model filtering
  // This field is expected to be initialized by the constructor externally in upstream changes.
  private List<TreePattern> exclusionPatterns;

  public TabletInsertionEventTreePatternParser(
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent,
      final InsertNode insertNode,
      final TreePattern pattern) {
    super(pipeTaskMeta, sourceEvent);
    this.pattern = pattern;

    if (insertNode instanceof InsertRowNode) {
      parse((InsertRowNode) insertNode);
    } else if (insertNode instanceof InsertTabletNode) {
      parse((InsertTabletNode) insertNode);
    } else {
      throw new UnSupportedDataTypeException(
          String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
    }
  }

  public TabletInsertionEventTreePatternParser(
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent,
      final Tablet tablet,
      final boolean isAligned,
      final TreePattern pattern) {
    super(pipeTaskMeta, sourceEvent);
    this.pattern = pattern;

    parse(tablet, isAligned);
  }

  @TestOnly
  public TabletInsertionEventTreePatternParser(
      final InsertNode insertNode, final TreePattern pattern) {
    this(null, null, insertNode, pattern);
  }

  @Override
  protected Object getPattern() {
    return pattern;
  }

  @Override
  protected void generateColumnIndexMapper(
      final String[] originMeasurementList,
      final Integer[] originColumnIndex2FilteredColumnIndexMapperList) {
    final int originColumnSize = originMeasurementList.length;

    // Helper to check exclusion
    final java.util.function.Predicate<String> excluded = m -> isMeasurementExcluded(deviceId, m);

    // case 1: for example, pattern is root.a.b or pattern is null and device is root.a.b.c
    // in this case, all data can be matched without checking the measurements
    if (Objects.isNull(pattern) || pattern.isRoot() || pattern.coversDevice(deviceId)) {
      int filteredCount = 0;
      for (int i = 0; i < originColumnSize; i++) {
        final String measurement = originMeasurementList[i];
        // ignore null measurement for partial insert
        if (measurement == null) {
          continue;
        }
        if (!excluded.test(measurement)) {
          originColumnIndex2FilteredColumnIndexMapperList[i] = filteredCount++;
        }
      }
    }

    // case 2: for example, pattern is root.a.b.c and device is root.a.b
    // in this case, we need to check the full path
    else if (pattern.mayOverlapWithDevice(deviceId)) {
      int filteredCount = 0;

      for (int i = 0; i < originColumnSize; i++) {
        final String measurement = originMeasurementList[i];

        // ignore null measurement for partial insert
        if (measurement == null) {
          continue;
        }

        if (pattern.matchesMeasurement(deviceId, measurement) && !excluded.test(measurement)) {
          originColumnIndex2FilteredColumnIndexMapperList[i] = filteredCount++;
        }
      }
    }
  }

  private boolean isMeasurementExcluded(
      final org.apache.tsfile.file.metadata.IDeviceID device, final String measurement) {
    if (Objects.isNull(exclusionPatterns) || exclusionPatterns.isEmpty()) {
      return false;
    }
    for (final TreePattern ex : exclusionPatterns) {
      if (Objects.isNull(ex)) {
        continue;
      }
      // If the exclusion covers the device, exclude all measurements
      if (ex.coversDevice(device)) {
        return true;
      }
      if (ex.mayOverlapWithDevice(device) && ex.matchesMeasurement(device, measurement)) {
        return true;
      }
    }
    return false;
  }

  ////////////////////////////  process  ////////////////////////////

  @Override
  public List<TabletInsertionEvent> processRowByRow(final BiConsumer<Row, RowCollector> consumer) {
    if (valueColumns.length == 0 || timestampColumn.length == 0) {
      return Collections.emptyList();
    }

    final PipeRowCollector rowCollector = new PipeRowCollector(pipeTaskMeta, sourceEvent);
    for (int i = 0; i < rowCount; i++) {
      consumer.accept(
          // Used for tree model
          new PipeRow(
              i,
              Objects.nonNull(deviceIdString) ? deviceIdString : deviceId.toString(),
              isAligned,
              measurementSchemaList,
              timestampColumn,
              valueColumnDataTypes,
              valueColumns,
              nullValueColumnBitmaps,
              columnNameStringList),
          rowCollector);
    }
    return rowCollector.convertToTabletInsertionEvents(shouldReport);
  }

  @Override
  public List<TabletInsertionEvent> processTablet(final BiConsumer<Tablet, RowCollector> consumer) {
    final PipeRowCollector rowCollector = new PipeRowCollector(pipeTaskMeta, sourceEvent);
    consumer.accept(convertToTablet(), rowCollector);
    return rowCollector.convertToTabletInsertionEvents(shouldReport);
  }

  ////////////////////////////  convertToTablet  ////////////////////////////

  @Override
  public Tablet convertToTablet() {
    if (tablet != null) {
      return tablet;
    }
    tablet =
        new Tablet(
            Objects.nonNull(deviceIdString) ? deviceIdString : deviceId.toString(),
            Arrays.asList(measurementSchemaList),
            timestampColumn,
            valueColumns,
            nullValueColumnBitmaps,
            rowCount);
    return tablet;
  }
}
