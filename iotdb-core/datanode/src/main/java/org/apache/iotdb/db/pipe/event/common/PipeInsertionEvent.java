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
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;

import javax.validation.constraints.NotNull;

/**
 * The data model used to record the Event and the data model of the DataRegion corresponding to the
 * source data, so this type requires some specifications .
 *
 * <p>1. {@code sourceDatabaseNameFromDataRegion} is immutable, coming from the source data or the
 * DataBaseName corresponding to the Processor that generates this Event.
 *
 * <p>2. {@code isTableModelEvent} is mutable, because it may be necessary to support the conversion
 * of the table model to the tree model or the tree model to the table model, leaving it to the user
 * to decide what model the data is. If it is not defined, the default is the data model
 * corresponding to {@code sourceDatabaseNameFromDataRegion}.
 *
 * <p>3. {@code treeModelDatabaseName} and {@code tableModelDatabaseName} are mutable, and the user
 * can change the name of the world, but it must correspond to the {@code isTableModelEvent} field.
 * The default is determined by {@code sourceDatabaseNameFromDataRegion}.
 *
 * <p>4. The corresponding {@link PipeTsFileInsertionEvent} cannot convert the data model at will,
 * and TSFile does not support this.
 */
public abstract class PipeInsertionEvent extends EnrichedEvent {

  // Record the database name of the DataRegion corresponding to the SourceEvent
  private final String sourceDatabaseNameFromDataRegion;

  protected Boolean isTableModelEvent; // lazy initialization

  protected String treeModelDatabaseName; // lazy initialization
  protected String tableModelDatabaseName; // lazy initialization
  protected boolean shouldParse4Privilege = false;

  protected PipeInsertionEvent(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final String userId,
      final String userName,
      final String clientHostname,
      final boolean skipIfNoPrivileges,
      final long startTime,
      final long endTime,
      final Boolean isTableModelEvent,
      final String databaseNameFromDataRegion,
      final String tableModelDatabaseName,
      final String treeModelDatabaseName) {
    super(
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        userId,
        userName,
        clientHostname,
        skipIfNoPrivileges,
        startTime,
        endTime);
    this.isTableModelEvent = isTableModelEvent;
    this.sourceDatabaseNameFromDataRegion = databaseNameFromDataRegion;
    this.treeModelDatabaseName = treeModelDatabaseName;
    if (tableModelDatabaseName != null) {
      this.tableModelDatabaseName = tableModelDatabaseName.toLowerCase();
    }
  }

  protected PipeInsertionEvent(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final String userId,
      final String userName,
      final String clientHostname,
      final boolean skipIfNoPrivileges,
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
        userId,
        userName,
        clientHostname,
        skipIfNoPrivileges,
        startTime,
        endTime,
        isTableModelEvent,
        databaseNameFromDataRegion,
        null,
        null);
  }

  public boolean isTableModelEvent() {
    if (isTableModelEvent == null) {
      if (sourceDatabaseNameFromDataRegion == null) {
        throw new IllegalStateException("databaseNameFromDataRegion is null");
      }
      return isTableModelEvent = PathUtils.isTableModelDatabase(sourceDatabaseNameFromDataRegion);
    }
    return isTableModelEvent;
  }

  public Boolean getRawIsTableModelEvent() {
    return isTableModelEvent;
  }

  public String getSourceDatabaseNameFromDataRegion() {
    return sourceDatabaseNameFromDataRegion;
  }

  public String getRawTableModelDataBase() {
    return tableModelDatabaseName;
  }

  public String getRawTreeModelDataBase() {
    return treeModelDatabaseName;
  }

  public String getTreeModelDatabaseName() {
    return treeModelDatabaseName == null
        ? treeModelDatabaseName = PathUtils.qualifyDatabaseName(sourceDatabaseNameFromDataRegion)
        : treeModelDatabaseName;
  }

  public String getTableModelDatabaseName() {
    return tableModelDatabaseName == null
        ? tableModelDatabaseName = PathUtils.unQualifyDatabaseName(sourceDatabaseNameFromDataRegion)
        : tableModelDatabaseName;
  }

  public void renameTableModelDatabase(@NotNull final String tableModelDatabaseName) {
    // Please note that if you parse TsFile, you need to use TreeModelDatabaseName, so you need to
    // rename TreeModelDatabaseName as well.
    this.tableModelDatabaseName = tableModelDatabaseName.toLowerCase();
    this.treeModelDatabaseName = PathUtils.qualifyDatabaseName(tableModelDatabaseName);
  }

  public boolean shouldParse4Privilege() {
    return shouldParse4Privilege;
  }
}
