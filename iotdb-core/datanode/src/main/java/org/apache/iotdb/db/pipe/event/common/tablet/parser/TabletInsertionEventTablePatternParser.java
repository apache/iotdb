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
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

public class TabletInsertionEventTablePatternParser extends TabletInsertionEventParser {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TabletInsertionEventTablePatternParser.class);

  private final TablePattern pattern;

  public TabletInsertionEventTablePatternParser(
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent,
      final InsertNode insertNode,
      final TablePattern pattern) {
    super(pipeTaskMeta, sourceEvent);
    this.pattern = pattern;

    if (insertNode instanceof RelationalInsertRowNode) {
      parse((RelationalInsertRowNode) insertNode);
    } else if (insertNode instanceof RelationalInsertTabletNode) {
      parse((RelationalInsertTabletNode) insertNode);
    } else {
      throw new UnSupportedDataTypeException(
          String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
    }
  }

  public TabletInsertionEventTablePatternParser(
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent,
      final Tablet tablet,
      final boolean isAligned,
      final TablePattern pattern) {
    super(pipeTaskMeta, sourceEvent);
    this.pattern = pattern;

    parse(tablet, isAligned);
  }

  @Override
  protected Object getPattern() {
    return pattern;
  }

  @Override
  protected void generateColumnIndexMapper(
      final String[] originMeasurementList,
      final Integer[] originColumnIndex2FilteredColumnIndexMapperList) {
    // In current implementation, the table pattern is already parsed.
    // We do not need to parse it again. But we keep the code here for future use.
    // TODO: Remove it if we do not need it in the future.
    final int originColumnSize = originMeasurementList.length;
    for (int i = 0; i < originColumnSize; i++) {
      originColumnIndex2FilteredColumnIndexMapperList[i] = i;
    }
  }

  ////////////////////////////  process  ////////////////////////////

  @Override
  public List<TabletInsertionEvent> processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.warn("TablePatternParser does not support row by row processing");
    }
    return Collections.emptyList();
  }

  @Override
  public List<TabletInsertionEvent> processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.warn("TablePatternParser does not support tablet processing");
    }
    return Collections.emptyList();
  }

  ////////////////////////////  convertToTablet  ////////////////////////////

  @Override
  public Tablet convertToTablet() {
    if (tablet != null) {
      return tablet;
    }

    final Tablet newTablet =
        new Tablet(
            Objects.nonNull(deviceIdString) ? deviceIdString : deviceId.getTableName(),
            Arrays.asList(measurementSchemaList),
            Arrays.asList(valueColumnTypes),
            timestampColumn,
            valueColumns,
            nullValueColumnBitmaps,
            rowCount);

    tablet = newTablet;

    return tablet;
  }
}
