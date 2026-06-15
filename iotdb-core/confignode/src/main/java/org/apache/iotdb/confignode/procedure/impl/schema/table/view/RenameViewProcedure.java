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

import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.TableSchemaObjectType;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RenameViewProcedure extends RenameTableProcedure {
  private TableSchemaObjectType viewObjectType;

  public RenameViewProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.viewObjectType = TableSchemaObjectType.VIEW;
  }

  public RenameViewProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String newName,
      final boolean isGeneratedByPipe) {
    this(database, tableName, queryId, newName, isGeneratedByPipe, TableSchemaObjectType.VIEW);
  }

  public RenameViewProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String newName,
      final boolean isGeneratedByPipe,
      final TableSchemaObjectType viewObjectType) {
    super(database, tableName, queryId, newName, isGeneratedByPipe);
    this.viewObjectType = viewObjectType;
  }

  @Override
  protected TableSchemaObjectType getTableSchemaObjectType() {
    return viewObjectType;
  }

  @Override
  protected String getActionMessage() {
    return "rename view";
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_RENAME_VIEW_PROCEDURE.getTypeCode()
            : ProcedureType.RENAME_VIEW_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }

  @Override
  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.innerSerialize(stream);
    stream.writeByte(viewObjectType.ordinal());
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.viewObjectType = TableSchemaObjectType.values()[byteBuffer.get()];
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RenameViewProcedure that = (RenameViewProcedure) o;
    return super.equals(o) && viewObjectType == that.viewObjectType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), viewObjectType);
  }
}
