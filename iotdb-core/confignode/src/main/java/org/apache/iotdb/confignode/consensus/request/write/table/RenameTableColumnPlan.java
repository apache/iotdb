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

package org.apache.iotdb.confignode.consensus.request.write.table;

import org.apache.iotdb.commons.utils.IOUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class RenameTableColumnPlan extends AbstractTablePlan {

  private List<String> oldNames;
  private List<String> newNames;
  private static final int MARKER_USE_NAME_LIST = -2;

  public RenameTableColumnPlan(final ConfigPhysicalPlanType type) {
    super(type);
  }

  public RenameTableColumnPlan(
      final String database,
      final String tableName,
      final List<String> oldNames,
      final List<String> newNames) {
    this(ConfigPhysicalPlanType.RenameTableColumn, database, tableName, oldNames, newNames);
  }

  protected RenameTableColumnPlan(
      final ConfigPhysicalPlanType type,
      final String database,
      final String tableName,
      final List<String> oldNames,
      final List<String> newNames) {
    super(type, database, tableName);
    this.oldNames = oldNames;
    this.newNames = newNames;
  }

  public List<String> getOldNames() {
    return oldNames;
  }

  public List<String> getNewNames() {
    return newNames;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    super.serializeImpl(stream);
    ReadWriteIOUtils.write(MARKER_USE_NAME_LIST, stream);
    IOUtils.write(oldNames, stream);
    IOUtils.write(newNames, stream);
  }

  @SuppressWarnings("UnnecessaryLocalVariable")
  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    super.deserializeImpl(buffer);
    int marker = ReadWriteIOUtils.readInt(buffer);
    if (marker != MARKER_USE_NAME_LIST) {
      int oldNameLen = marker;
      byte[] oldNamesBytes = new byte[oldNameLen];
      buffer.get(oldNamesBytes);
      String oldName = new String(oldNamesBytes, StandardCharsets.UTF_8);
      String newName = ReadWriteIOUtils.readString(buffer);
      this.oldNames = Collections.singletonList(oldName);
      this.newNames = Collections.singletonList(newName);
    } else {
      this.oldNames = IOUtils.readStringList(buffer);
      this.newNames = IOUtils.readStringList(buffer);
    }
  }
}
