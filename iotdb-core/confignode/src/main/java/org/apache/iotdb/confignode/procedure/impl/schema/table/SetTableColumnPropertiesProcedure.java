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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableColumnPropertiesPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class SetTableColumnPropertiesProcedure extends AbstractSetPropertiesProcedure {

  private String columnName;

  public SetTableColumnPropertiesProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public SetTableColumnPropertiesProcedure(
      final String database,
      final String tableName,
      final String columnName,
      final String queryId,
      final Map<String, String> properties,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, properties, isGeneratedByPipe);
    this.columnName = columnName;
  }

  @Override
  protected Pair<TSStatus, TsTable> updateProperties(final ConfigNodeProcedureEnv env)
      throws MetadataException {
    return env.getConfigManager()
        .getClusterSchemaManager()
        .updateTableColumnProperties(
            database, tableName, columnName, originalProperties, updatedProperties);
  }

  @Override
  protected ConfigPhysicalPlan createSetPropertiesPlan(
      final Map<String, String> properties, final boolean isRollback) {
    return new SetTableColumnPropertiesPlan(
        database, tableName, columnName, properties, isRollback);
  }

  @Override
  protected String getActionMessage() {
    return "set table column properties";
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_SET_TABLE_COLUMN_PROPERTIES_PROCEDURE.getTypeCode()
            : ProcedureType.SET_TABLE_COLUMN_PROPERTIES_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(columnName, stream);
    serializeProperties(stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.columnName = ReadWriteIOUtils.readString(byteBuffer);
    deserializeProperties(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(columnName, ((SetTableColumnPropertiesProcedure) o).columnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), columnName);
  }
}
