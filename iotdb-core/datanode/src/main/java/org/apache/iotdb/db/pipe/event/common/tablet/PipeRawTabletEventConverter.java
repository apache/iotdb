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
import org.apache.iotdb.pipe.api.collector.DataCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import java.util.ArrayList;
import java.util.List;

public abstract class PipeRawTabletEventConverter implements DataCollector {
  protected final List<TabletInsertionEvent> tabletInsertionEventList = new ArrayList<>();
  protected boolean isAligned = false;
  protected final PipeTaskMeta pipeTaskMeta; // Used to report progress
  protected final EnrichedEvent sourceEvent; // Used to report progress
  protected String sourceEventDataBaseName;
  protected Boolean isTableModel;
  protected String rawTableModelDataBaseName;
  protected String rawTreeModelDataBaseName;

  public PipeRawTabletEventConverter(PipeTaskMeta pipeTaskMeta, EnrichedEvent sourceEvent) {
    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceEvent = sourceEvent;
    if (sourceEvent instanceof PipeInsertionEvent) {
      final PipeInsertionEvent pipeInsertionEvent = (PipeInsertionEvent) sourceEvent;
      sourceEventDataBaseName = pipeInsertionEvent.getSourceDatabaseNameFromDataRegion();
      isTableModel = pipeInsertionEvent.getRawIsTableModelEvent();
      rawTableModelDataBaseName = pipeInsertionEvent.getRawTableModelDataBase();
      rawTreeModelDataBaseName = pipeInsertionEvent.getRawTreeModelDataBase();
    } else {
      sourceEventDataBaseName = null;
      isTableModel = null;
      rawTableModelDataBaseName = null;
      rawTreeModelDataBaseName = null;
    }
  }

  public PipeRawTabletEventConverter(
      PipeTaskMeta pipeTaskMeta,
      EnrichedEvent sourceEvent,
      String sourceEventDataBase,
      Boolean isTableModel) {
    this(pipeTaskMeta, sourceEvent);
    this.sourceEventDataBaseName = sourceEventDataBase;
    this.isTableModel = isTableModel;
  }

  public PipeRawTabletEventConverter(
      PipeTaskMeta pipeTaskMeta,
      EnrichedEvent sourceEvent,
      String sourceEventDataBase,
      Boolean isTableModel,
      String rawTableModelDataBaseName,
      String rawTreeModelDataBaseName) {
    this(pipeTaskMeta, sourceEvent, sourceEventDataBase, isTableModel);
    this.rawTableModelDataBaseName = rawTableModelDataBaseName;
    this.rawTreeModelDataBaseName = rawTreeModelDataBaseName;
  }

  public void resetDatabaseInfo(
      final String sourceEventDataBaseName,
      final Boolean isTableModel,
      final String rawTableModelDataBaseName,
      final String rawTreeModelDataBaseName) {
    this.sourceEventDataBaseName = sourceEventDataBaseName;
    this.isTableModel = isTableModel;
    this.rawTableModelDataBaseName = rawTableModelDataBaseName;
    this.rawTreeModelDataBaseName = rawTreeModelDataBaseName;
  }

  @Override
  public List<TabletInsertionEvent> convertToTabletInsertionEvents(final boolean shouldReport) {
    final int eventListSize = tabletInsertionEventList.size();
    if (eventListSize > 0 && shouldReport) { // The last event should report progress
      ((PipeRawTabletInsertionEvent) tabletInsertionEventList.get(eventListSize - 1))
          .markAsNeedToReport();
    }
    return tabletInsertionEventList;
  }
}
