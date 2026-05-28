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

package com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable;

import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AlterTableColumnDataTypeProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.TableSchemaObjectType;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;

public class AlterWritableViewColumnDataTypeProcedure extends AlterTableColumnDataTypeProcedure {
  public AlterWritableViewColumnDataTypeProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AlterWritableViewColumnDataTypeProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String columnName,
      final TSDataType dataType,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, columnName, dataType, isGeneratedByPipe);
  }

  @Override
  protected TableSchemaObjectType getTableSchemaObjectType() {
    return TableSchemaObjectType.WRITABLE_VIEW;
  }

  @Override
  protected String getActionMessage() {
    return ProcedureMessages.ALTER_WRITABLE_VIEW_COLUMN_DATA_TYPE_PROCEDURE;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_ALTER_WRITABLE_VIEW_COLUMN_DATATYPE_PROCEDURE
                .getTypeCode()
            : ProcedureType.ALTER_WRITABLE_VIEW_COLUMN_DATATYPE_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }
}
