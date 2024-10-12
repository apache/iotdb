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

package org.apache.iotdb.commons.pipe.event;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;

public abstract class PipeInsertionEvent extends EnrichedEvent {

  private boolean isTableModel;
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
      final String treeModelDatabaseName,
      final String tableModelDatabaseName,
      final boolean isTableModel) {
    super(pipeName, creationTime, pipeTaskMeta, treePattern, tablePattern, startTime, endTime);
    this.treeModelDatabaseName = treeModelDatabaseName;
    this.tableModelDatabaseName = tableModelDatabaseName;
    this.isTableModel = isTableModel;
  }

  protected PipeInsertionEvent(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final String databaseNameFromDataRegion,
      final boolean isTableModel) {
    this(
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        startTime,
        endTime,
        databaseNameFromDataRegion,
        null,
        isTableModel);
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

  /**
   * Checks if the current object represents a table model.
   *
   * <p>This method can only be reliably used after the extractor has been run. Before the
   * extractor's execution, it's not possible to accurately determine if the table model is matched
   * or not.
   *
   * @return true if the object is a table model, false is a tree model.
   */
  public boolean isTableModel() {
    return isTableModel;
  }

  public void setModelType(boolean isTableModel) {
    this.isTableModel = isTableModel;
  }
}
