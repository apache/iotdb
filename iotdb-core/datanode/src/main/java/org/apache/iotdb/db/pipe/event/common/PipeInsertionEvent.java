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
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import javax.validation.constraints.NotNull;

import java.util.Collections;
import java.util.Iterator;

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
      final String cliHostname,
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
        cliHostname,
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
      final String cliHostname,
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
        cliHostname,
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
        throw new IllegalStateException(DataNodePipeMessages.DATABASENAMEFROMDATAREGION_IS_NULL);
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

  /////////////////////////// Object-type data ///////////////////////////

  /**
   * Returns an iterator over object file paths (relative paths resolved by TierManager) for this
   * event. Events that may contain Object-type data should override this and return a non-empty
   * iterator. Callers can use this to link or process paths one-by-one without building a full
   * list. Default implementation returns an empty iterator.
   *
   * @return iterator over object paths; never null
   */
  public Iterator<String> objectPathIterator() {
    return Collections.emptyIterator();
  }

  /**
   * Set whether the event has Object-type data manually. This can be used to manually mark Object
   * data without scanning. Default implementation does nothing. Only events that may contain Object
   * types need to override this.
   *
   * @param hasObject whether the event has Object-type data; {@code null} means leave the flag
   *     unchanged (unknown / not yet determined)
   */
  public void setHasObject(final Boolean hasObject) {
    // Default implementation does nothing
  }

  /**
   * Check if the event contains any Object-type data. Default implementation returns false. Only
   * events that may contain Object types need to override this.
   *
   * @return true if the event contains Object-type data, false otherwise
   */
  public boolean hasObjectData() {
    return false;
  }

  /////////////////////////// TsFile resource (Object file linkage) ///////////////////////////

  /**
   * Get the TsFileResource associated with this event. Default implementation returns null. Only
   * events that are associated with a TsFile need to override this.
   *
   * @return TsFileResource if this event is associated with a TsFile, null otherwise
   */
  public TsFileResource getTsFileResource() {
    return null;
  }

  /**
   * Set the TsFileResource for this event. Default implementation does nothing. Only events that
   * need to store TsFileResource need to override this.
   *
   * @param tsFileResource the TsFileResource to set
   */
  public void setTsFileResource(final TsFileResource tsFileResource) {
    // Default implementation does nothing
  }
}
