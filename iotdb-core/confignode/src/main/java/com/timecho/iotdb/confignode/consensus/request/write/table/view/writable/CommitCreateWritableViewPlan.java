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

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.table.AbstractTablePlan;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CommitCreateWritableViewPlan extends AbstractTablePlan {

  private TsTable originalTable;

  public CommitCreateWritableViewPlan() {
    super(ConfigPhysicalPlanType.CommitCreateWritableView);
  }

  public CommitCreateWritableViewPlan(
      final String database,
      final String tableName,
      final @Nullable String originalDatabase,
      final @Nullable TsTable originalTable) {
    super(ConfigPhysicalPlanType.CommitCreateWritableView, database, tableName);
    if (Objects.nonNull(originalDatabase) != Objects.nonNull(originalTable)) {
      throw new IllegalArgumentException(
          "originalDatabase and originalTable must either both be set or both be null");
    }
    this.originalDatabase = originalDatabase;
    this.originalTableName = Objects.nonNull(originalTable) ? originalTable.getTableName() : null;
    this.originalTable = originalTable;
  }

  public TsTable getOriginalTable() {
    return originalTable;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    super.serializeImpl(stream);

    ReadWriteIOUtils.write(originalDatabase, stream);
    // Only schema-cascade writable views need the source table snapshot in the commit plan.
    if (Objects.nonNull(originalTable)) {
      originalTable.serialize(stream);
    }
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    super.deserializeImpl(buffer);

    this.originalDatabase = ReadWriteIOUtils.readString(buffer);
    if (Objects.nonNull(originalDatabase)) {
      originalTable = TsTable.deserialize(buffer);
      originalTableName = originalTable.getTableName();
    }
  }
}
