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

import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.TableSchemaObjectType;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SetWritableViewPropertiesProcedure extends SetTablePropertiesProcedure {

  private transient SetTablePropertiesProcedure sourceProcedure;
  private transient boolean sourceProcedureInitialized;

  // Used for ser/de, indicates whether the source procedure should run based on the old cascade.
  private Boolean shouldRunSourceProcedure = null;

  public SetWritableViewPropertiesProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public SetWritableViewPropertiesProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final Map<String, String> properties,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, properties, isGeneratedByPipe);
  }

  public SetWritableViewPropertiesProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final Map<String, String> properties,
      final boolean isGeneratedByPipe,
      final Boolean shouldRunSourceProcedure) {
    this(database, tableName, queryId, properties, isGeneratedByPipe);
    this.shouldRunSourceProcedure = shouldRunSourceProcedure;
  }

  @Override
  protected TableSchemaObjectType getTableSchemaObjectType() {
    return TableSchemaObjectType.WRITABLE_VIEW;
  }

  @Override
  protected String getActionMessage() {
    return ProcedureMessages.SET_WRITABLE_VIEW_PROPERTIES;
  }

  @Override
  public void validateTable(final ConfigNodeProcedureEnv env) {
    mayInitOriginal();
    super.validateTable(env);
    WritableViewUtils.executeForSource(
        this,
        sourceProcedure,
        () -> sourceProcedure.validateTable(env),
        Boolean.TRUE.equals(shouldRunSourceProcedure));
    if (!isFailed()
        && Objects.nonNull(sourceProcedure)
        && Objects.nonNull(sourceProcedure.getTable())) {
      originalTable = sourceProcedure.getTable();
    }
  }

  @Override
  protected void mayInitOriginal() {
    super.mayInitOriginal();
    if (Objects.isNull(shouldRunSourceProcedure)) {
      shouldRunSourceProcedure = Objects.nonNull(originalDatabase);
    }
    if (sourceProcedureInitialized) {
      return;
    }
    sourceProcedureInitialized = true;
    if (Boolean.TRUE.equals(shouldRunSourceProcedure) && Objects.nonNull(originalDatabase)) {
      final Map<String, String> updatedProperties = new HashMap<>(this.updatedProperties);
      updatedProperties.remove(WritableView.SCHEMA_CASCADE);
      sourceProcedure =
          new SetTablePropertiesProcedure(
              originalDatabase,
              originalTable.getTableName(),
              queryId + "_source",
              updatedProperties,
              this.isGeneratedByPipe);
      sourceProcedure.setTable(originalTable);
    }
  }

  @Override
  protected Boolean getSourceResolutionOverride() {
    if (Objects.nonNull(shouldRunSourceProcedure)) {
      return shouldRunSourceProcedure;
    }
    return null;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_SET_WRITABLE_VIEW_PROPERTIES_PROCEDURE.getTypeCode()
            : ProcedureType.SET_WRITABLE_VIEW_PROPERTIES_PROCEDURE.getTypeCode());
    innerSerialize(stream);

    // We need to serialize whether there is a source procedure
    // Consider the case where the ttl and schema cascade are both unset
    // The table need to change ttl, but if the schema cascade is already set to false
    // If the procedure is rolled back or committed, and the configNode is down
    // If we do not restore the source procedure this case, it won't be able to roll back or
    // commit
    if (Objects.isNull(shouldRunSourceProcedure)) {
      mayInitOriginal();
    }
    ReadWriteIOUtils.write(shouldRunSourceProcedure, stream);
  }

  @Override
  public void deserialize(final ByteBuffer buffer) {
    super.deserialize(buffer);
    shouldRunSourceProcedure = ReadWriteIOUtils.readBoolObject(buffer);
  }
}
