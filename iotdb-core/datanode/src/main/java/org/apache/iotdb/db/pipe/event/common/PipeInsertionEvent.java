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

package org.apache.iotdb.db.pipe.event.common;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

public abstract class PipeInsertionEvent extends EnrichedEvent {

  private Boolean isTableModelEvent; // lazy initialization

  private final String treeModelDatabaseName;
  private String tableModelDatabaseName; // lazy initialization

  protected PipeInsertionEvent(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final Boolean isTableModelEvent,
      final String treeModelDatabaseName,
      final String tableModelDatabaseName) {
    super(pipeName, creationTime, pipeTaskMeta, treePattern, tablePattern, startTime, endTime);
    this.isTableModelEvent = isTableModelEvent;
    this.treeModelDatabaseName = treeModelDatabaseName;
    this.tableModelDatabaseName = tableModelDatabaseName;
  }

  protected PipeInsertionEvent(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final Boolean isTableModelEvent,
      final String databaseNameFromDataRegion) {
    this(
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        startTime,
        endTime,
        isTableModelEvent,
        databaseNameFromDataRegion,
        null);
  }

  public void markAsTableModelEvent() {
    isTableModelEvent = Boolean.TRUE;
  }

  public void markAsTreeModelEvent() {
    isTableModelEvent = Boolean.FALSE;
  }

  public boolean isTableModelEvent() {
    if (isTableModelEvent == null) {
      throw new IllegalStateException("isTableModelEvent is not initialized");
    }
    return isTableModelEvent;
  }

  /** Only for internal use. */
  protected Boolean getRawIsTableModelEvent() {
    return isTableModelEvent;
  }

  public String getTreeModelDatabaseName() {
    return treeModelDatabaseName;
  }

  public String getTableModelDatabaseName() {
    return tableModelDatabaseName == null
        ? tableModelDatabaseName =
            treeModelDatabaseName != null && treeModelDatabaseName.startsWith("root.")
                ? treeModelDatabaseName.substring(5)
                : treeModelDatabaseName
        : tableModelDatabaseName;
  }

  public void renameTableModelDatabase(final String tableModelDatabaseName) {
    this.tableModelDatabaseName = tableModelDatabaseName;
  }
}
