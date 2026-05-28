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

package com.timecho.iotdb.confignode.consensus.request.write.table.view.writable;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RenameWritableViewColumnPlan extends RenameTableColumnPlan {
  private @Nullable String originalOldName;
  private @Nullable String originalNewName;

  public RenameWritableViewColumnPlan() {
    super(ConfigPhysicalPlanType.RenameWritableViewColumn);
  }

  public RenameWritableViewColumnPlan(
      final String database, final String tableName, final String oldName, final String newName) {
    super(ConfigPhysicalPlanType.RenameWritableViewColumn, database, tableName, oldName, newName);
  }

  public RenameWritableViewColumnPlan(
      final String database,
      final String tableName,
      final String oldName,
      final String newName,
      final @Nullable String originalDatabase,
      final @Nullable String originalTableName,
      final @Nullable String originalOldName,
      final @Nullable String originalNewName) {
    super(ConfigPhysicalPlanType.RenameWritableViewColumn, database, tableName, oldName, newName);
    this.originalDatabase = originalDatabase;
    this.originalTableName = originalTableName;
    this.originalOldName = originalOldName;
    this.originalNewName = originalNewName;
  }

  @Nullable
  public String getOriginalOldName() {
    return originalOldName;
  }

  @Nullable
  public String getOriginalNewName() {
    return originalNewName;
  }

  @Override
  protected boolean needOriginal() {
    return true;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    super.serializeImpl(stream);
    ReadWriteIOUtils.write(originalOldName, stream);
    ReadWriteIOUtils.write(originalNewName, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    super.deserializeImpl(buffer);
    if (buffer.hasRemaining()) {
      this.originalOldName = ReadWriteIOUtils.readString(buffer);
      this.originalNewName = ReadWriteIOUtils.readString(buffer);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RenameWritableViewColumnPlan)) {
      return false;
    }
    final RenameWritableViewColumnPlan that = (RenameWritableViewColumnPlan) o;
    return super.equals(o)
        && Objects.equals(getOldName(), that.getOldName())
        && Objects.equals(getNewName(), that.getNewName())
        && Objects.equals(originalDatabase, that.originalDatabase)
        && Objects.equals(originalTableName, that.originalTableName)
        && Objects.equals(originalOldName, that.originalOldName)
        && Objects.equals(originalNewName, that.originalNewName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        getOldName(),
        getNewName(),
        originalDatabase,
        originalTableName,
        originalOldName,
        originalNewName);
  }
}
