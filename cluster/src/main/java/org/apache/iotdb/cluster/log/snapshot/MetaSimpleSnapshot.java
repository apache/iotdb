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

package org.apache.iotdb.cluster.log.snapshot;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.utils.SerializeUtils;

/**
 * MetaSimpleSnapshot also records all storage groups.
 */
public class MetaSimpleSnapshot extends SimpleSnapshot{

  private List<String> storageGroups;

  public MetaSimpleSnapshot() {
  }

  public MetaSimpleSnapshot(List<Log> snapshot, List<String> storageGroups) {
    super(snapshot);
    this.storageGroups = storageGroups;
  }

  public List<String> getStorageGroups() {
    return storageGroups;
  }

  @Override
  void subSerialize(DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(storageGroups.size());
      for (String storageGroup : storageGroups) {
        SerializeUtils.serialize(storageGroup, dataOutputStream);
      }
    } catch (IOException e) {
      // unreachable
    }
  }

  @Override
  void subDeserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    storageGroups = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      storageGroups.add(SerializeUtils.deserializeString(buffer));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MetaSimpleSnapshot snapshot = (MetaSimpleSnapshot) o;
    return Objects.equals(storageGroups, snapshot.storageGroups);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), storageGroups);
  }
}
