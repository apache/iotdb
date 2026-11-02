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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.schema.table.TreeToTableViewTrieOperationType;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.persistence.schema.TreeDeviceViewFieldDetector;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTableReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTree2TableView;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.commons.schema.table.TreeToTableViewTrieOperationType.ALTER_TAG;
import static org.apache.iotdb.commons.schema.table.TsTable.AUTO_SYNC_SCHEMA;

public class AddViewColumnProcedure extends AddTableColumnProcedure {
  public AddViewColumnProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AddViewColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final List<TsTableColumnSchema> addedColumnList,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, addedColumnList, isGeneratedByPipe);
  }

  @Override
  protected void columnCheck(final ConfigNodeProcedureEnv env) {
    super.columnCheck(env);
    // Check failure
    if (getState().equals(ProcedureState.FAILED)) {
      return;
    }

    final Map<String, FieldColumnSchema> fields2Detect = new HashMap<>();
    for (final TsTableColumnSchema schema : addedColumnList) {
      if (!(schema instanceof FieldColumnSchema) || schema.getDataType() != TSDataType.UNKNOWN) {
        continue;
      }
      final String key = TreeViewSchema.getSourceName(schema);
      if (fields2Detect.containsKey(key)) {
        // The column check has already dropped the columns which refer to an already referenced
        // source measurement, so the added columns must not share one.
        throw new SemanticException(
            String.format(
                ManagerMessages
                    .EXCEPTION_THE_DUPLICATED_SOURCE_MEASUREMENT_ARG_IS_UNSUPPORTED_YET_873AF566,
                key));
      }
      fields2Detect.put(key, (FieldColumnSchema) schema);
    }

    if (!fields2Detect.isEmpty()) {
      final TSStatus status =
          new TreeDeviceViewFieldDetector(env.getConfigManager(), table)
              .detectFieldTypesForColumnAddition(fields2Detect);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        setFailure(new ProcedureException(new IoTDBException(status)));
      }
    }
  }

  @Override
  protected TUpdateTableReq buildCommitReleaseTableReq(final @Nullable String oldName) {
    final Optional<TreeToTableViewTrieOperationType> type = tree2TableViewOperationType();
    if (type.isEmpty()) {
      return super.buildCommitReleaseTableReq(oldName);
    }
    // Adding a tag column changes the tag count a device path is matched with.
    final TUpdateTree2TableView viewInfo = new TUpdateTree2TableView(type.get().getOperationType());
    viewInfo.setTagNum(table.getTagNum());
    return SchemaUtils.buildCommitUpdateTableReq(database, table.getTableName(), oldName, viewInfo);
  }

  private Optional<TreeToTableViewTrieOperationType> tree2TableViewOperationType() {
    final Optional<String> propValue = table.getPropValue(AUTO_SYNC_SCHEMA);
    if (propValue.isPresent() && Boolean.parseBoolean(propValue.get())) {
      final boolean hasTagColumn =
          addedColumnList.stream().anyMatch(c -> c instanceof TagColumnSchema);
      return hasTagColumn ? Optional.of(ALTER_TAG) : Optional.empty();
    }
    return Optional.empty();
  }

  @Override
  protected String getActionMessage() {
    return "add view column";
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_ADD_VIEW_COLUMN_PROCEDURE.getTypeCode()
            : ProcedureType.ADD_VIEW_COLUMN_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }
}
