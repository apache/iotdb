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
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.write.record.Tablet;

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

public class TabletInsertionEventTablePatternParser implements TabletInsertionEventParser {

  private final PipeTaskMeta pipeTaskMeta;
  private final EnrichedEvent sourceEvent;
  private final InsertNode insertNode;
  private final TablePattern pattern;

  // Whether the parser shall report progress
  private boolean shouldReport = false;

  public TabletInsertionEventTablePatternParser(
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent,
      final InsertNode insertNode,
      final TablePattern pattern) {
    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceEvent = sourceEvent;
    this.insertNode = insertNode;
    this.pattern = pattern;
  }

  @Override
  public boolean isAligned() {
    return true;
  }

  @Override
  public void markAsNeedToReport() {
    shouldReport = true;
  }

  @Override
  public List<TabletInsertionEvent> processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    return Collections.emptyList();
  }

  @Override
  public List<TabletInsertionEvent> processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    return Collections.emptyList();
  }

  @Override
  public Tablet convertToTablet() {
    return null;
  }
}
