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

package org.apache.iotdb.confignode.procedure.impl.schema.table.view;

import org.apache.iotdb.commons.schema.table.TreeToTableViewTrieOperationType;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.confignode.persistence.schema.TreeDeviceViewFieldDetector;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTableReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTree2TableView;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.commons.schema.table.TsTable.AUTO_SYNC_SCHEMA;

public class SetViewPropertiesProcedure extends SetTablePropertiesProcedure {
  public SetViewPropertiesProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public SetViewPropertiesProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final Map<String, String> properties,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, properties, isGeneratedByPipe);
  }

  @Override
  protected TUpdateTableReq buildCommitReleaseTableReq(final @Nullable String oldName) {
    final Optional<TreeToTableViewTrieOperationType> type = tree2TableViewOperationType();
    if (type.isEmpty()) {
      return super.buildCommitReleaseTableReq(oldName);
    }
    final TUpdateTree2TableView viewInfo = new TUpdateTree2TableView(type.get().getOperationType());
    if (TreeToTableViewTrieOperationType.ALTER_AUTO_SYNC == type.get()) {
      // Turning the auto detection on mounts the view: the DataNode needs its whole definition.
      table.getPropValue(TreeViewSchema.TREE_PATH_PATTERN).ifPresent(viewInfo::setTreePath);
      viewInfo.setIsRestrict(TreeViewSchema.isRestrict(table));
      viewInfo.setTagNum(table.getTagNum());
    }
    // turn down the auto_sync_schema
    return SchemaUtils.buildCommitUpdateTableReq(database, table.getTableName(), oldName, viewInfo);
  }

  @Override
  protected void commitReleaseWithHA(final ConfigNodeProcedureEnv env) {
    super.commitReleaseWithHA(env);
    // Turning the auto detection on mounts the view, so the series which already exist under its
    // path pattern need the same second detection as the ones of a newly created view.
    if (TreeToTableViewTrieOperationType.ALTER_AUTO_SYNC
        == tree2TableViewOperationType().orElse(null)) {
      reconcileTree2TableViewColumns(env);
    }
  }

  /** Adds the columns which the view, just mounted by the commit, is still missing. */
  private void reconcileTree2TableViewColumns(final ConfigNodeProcedureEnv env) {
    final List<TsTableColumnSchema> missingColumns =
        TreeDeviceViewFieldDetector.detectMissingColumnsAfterCreation(
            env.getConfigManager(), database, table);
    if (missingColumns.isEmpty()) {
      return;
    }
    addChildProcedure(
        new AddViewColumnProcedure(
            database,
            table.getTableName(),
            String.format("set_view_properties_reconcile_%s.%s", database, table.getTableName()),
            missingColumns,
            false));
  }

  /**
   * The effect of the new properties on the tree-to-table view index, or empty when they do not
   * change it. Turning auto_sync_schema on registers the view, turning it off unregisters it, and
   * any other property leaves the index alone.
   */
  private Optional<TreeToTableViewTrieOperationType> tree2TableViewOperationType() {
    if (!updatedProperties.containsKey(AUTO_SYNC_SCHEMA)) {
      return Optional.empty();
    }
    final TreeToTableViewTrieOperationType type =
        indexEffectOf(updatedProperties.get(AUTO_SYNC_SCHEMA));
    // An absent auto_sync_schema means false, so setting it to false is not a change.
    return type == indexEffectOf(originalProperties.get(AUTO_SYNC_SCHEMA))
        ? Optional.empty()
        : Optional.of(type);
  }

  private static TreeToTableViewTrieOperationType indexEffectOf(
      final @Nullable String autoSyncSchema) {
    return Boolean.parseBoolean(autoSyncSchema)
        ? TreeToTableViewTrieOperationType.ALTER_AUTO_SYNC
        : TreeToTableViewTrieOperationType.REMOVE;
  }

  @Override
  protected String getActionMessage() {
    return "set view properties";
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_SET_VIEW_PROPERTIES_PROCEDURE.getTypeCode()
            : ProcedureType.SET_VIEW_PROPERTIES_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }
}
