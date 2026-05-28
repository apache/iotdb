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

import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.table.view.PreCreateTableViewPlan;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RollbackCreateWritableViewPlan extends PreCreateTableViewPlan {
  private String originalDatabase;
  private TsTable originalTable;
  private boolean restoreView;

  public RollbackCreateWritableViewPlan() {
    super(ConfigPhysicalPlanType.RollbackCreateWritableView);
  }

  public RollbackCreateWritableViewPlan(
      final String database,
      final TsTable table,
      final TableNodeStatus status,
      final String originalDatabase,
      final TsTable originalTable) {
    this(database, table, status, originalDatabase, originalTable, false);
  }

  public RollbackCreateWritableViewPlan(
      final String database,
      final TsTable table,
      final TableNodeStatus status,
      final String originalDatabase,
      final TsTable originalTable,
      final boolean restoreView) {
    super(ConfigPhysicalPlanType.RollbackCreateWritableView, database, table, status);
    if (Objects.nonNull(originalDatabase) != Objects.nonNull(originalTable)) {
      throw new IllegalArgumentException(
          "originalDatabase and originalTable must either both be set or both be null");
    }
    this.originalDatabase = originalDatabase;
    this.originalTable = originalTable;
    this.restoreView = restoreView;
  }

  public String getOriginalDatabase() {
    return originalDatabase;
  }

  public TsTable getOriginalTable() {
    return originalTable;
  }

  public boolean isRestoreView() {
    return restoreView;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    super.serializeImpl(stream);
    ReadWriteIOUtils.write(originalDatabase, stream);
    if (Objects.nonNull(originalTable)) {
      originalTable.serialize(stream);
    }
    ReadWriteIOUtils.write(restoreView, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    super.deserializeImpl(buffer);
    if (!buffer.hasRemaining()) {
      return;
    }
    originalDatabase = ReadWriteIOUtils.readString(buffer);
    if (Objects.nonNull(originalDatabase)) {
      originalTable = TsTable.deserialize(buffer);
    }
    if (buffer.hasRemaining()) {
      restoreView = ReadWriteIOUtils.readBool(buffer);
    }
  }
}
