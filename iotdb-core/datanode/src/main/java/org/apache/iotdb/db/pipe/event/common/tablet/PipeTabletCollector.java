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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.pipe.api.collector.TabletCollector;

import org.apache.tsfile.write.record.Tablet;

public class PipeTabletCollector extends PipeRawTabletEventConverter implements TabletCollector {

  public PipeTabletCollector(PipeTaskMeta pipeTaskMeta, EnrichedEvent sourceEvent) {
    super(pipeTaskMeta, sourceEvent);
  }

  public PipeTabletCollector(
      PipeTaskMeta pipeTaskMeta,
      EnrichedEvent sourceEvent,
      String sourceEventDataBase,
      Boolean isTableModel) {
    super(pipeTaskMeta, sourceEvent, sourceEventDataBase, isTableModel);
  }

  @Override
  public void collectTablet(final Tablet tablet) {
    final PipeInsertionEvent pipeInsertionEvent =
        sourceEvent instanceof PipeInsertionEvent ? ((PipeInsertionEvent) sourceEvent) : null;
    tabletInsertionEventList.add(
        new PipeRawTabletInsertionEvent(
            isTableModel,
            sourceEventDataBaseName,
            pipeInsertionEvent == null ? null : pipeInsertionEvent.getRawTableModelDataBase(),
            pipeInsertionEvent == null ? null : pipeInsertionEvent.getRawTreeModelDataBase(),
            tablet,
            isAligned,
            sourceEvent == null ? null : sourceEvent.getPipeName(),
            sourceEvent == null ? 0 : sourceEvent.getCreationTime(),
            pipeTaskMeta,
            sourceEvent,
            false));
  }
}
