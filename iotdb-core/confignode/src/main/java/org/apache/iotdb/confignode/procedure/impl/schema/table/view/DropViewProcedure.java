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

import org.apache.iotdb.commons.schema.table.TreeToTableViewOperationType;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTableReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTree2TableView;

import java.io.DataOutputStream;
import java.io.IOException;

public class DropViewProcedure extends DropTableProcedure {
  public DropViewProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DropViewProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, isGeneratedByPipe);
  }

  /** A dropped view has to leave the tree-to-table view index of every DataNode. */
  @Override
  protected TUpdateTableReq buildCommitReleaseTableReq(final String oldName) {
    // Unregistering a view needs nothing but its name, which the request already carries.
    return SchemaUtils.buildCommitUpdateTableReq(
        database,
        table.getTableName(),
        null,
        new TUpdateTree2TableView(TreeToTableViewOperationType.REMOVE.getOperationType()));
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_DROP_VIEW_PROCEDURE.getTypeCode()
            : ProcedureType.DROP_VIEW_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }
}
