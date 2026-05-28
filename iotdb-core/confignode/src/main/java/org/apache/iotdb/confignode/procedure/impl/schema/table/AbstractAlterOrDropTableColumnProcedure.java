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

import org.apache.iotdb.commons.schema.table.WritableView;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class AbstractAlterOrDropTableColumnProcedure<T>
    extends AbstractAlterOrDropTableProcedure<T> {

  protected String columnName;

  protected AbstractAlterOrDropTableColumnProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  protected AbstractAlterOrDropTableColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String columnName,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, isGeneratedByPipe);
    this.columnName = columnName;
  }

  protected String getOriginalColumnName() {
    return table instanceof WritableView && Objects.nonNull(getOriginalDatabase())
        ? ((WritableView) table).getOriginalColumnName(columnName)
        : null;
  }

  protected boolean hasOriginalColumn() {
    return Objects.nonNull(getOriginalColumnName());
  }

  protected String getOriginalDatabaseForColumn() {
    return hasOriginalColumn() ? getOriginalDatabase() : null;
  }

  protected String getOriginalTableNameForColumn() {
    return hasOriginalColumn() ? getOriginalTableName() : null;
  }

  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(columnName, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.columnName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(columnName, ((AbstractAlterOrDropTableColumnProcedure<?>) o).columnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), columnName);
  }
}
