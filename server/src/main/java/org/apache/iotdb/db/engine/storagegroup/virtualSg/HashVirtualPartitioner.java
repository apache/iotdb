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
package org.apache.iotdb.db.engine.storagegroup.virtualSg;

import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;

public class HashVirtualPartitioner implements VirtualPartitioner {

  public static final int STORAGE_GROUP_NUM = IoTDBDescriptor.getInstance().getConfig()
      .getVirtualPartitionNum();

  // storage id -> set (device id)
  private final Set<PartialPath>[] sgToDevice;

  private HashVirtualPartitioner() {
    sgToDevice = new Set[STORAGE_GROUP_NUM];
    for (int i = 0; i < STORAGE_GROUP_NUM; i++) {
      sgToDevice[i] = new HashSet<>();
    }
  }

  public static HashVirtualPartitioner getInstance() {
    return HashVirtualPartitionerHolder.INSTANCE;
  }

  @Override
  public PartialPath deviceToStorageGroup(PartialPath deviceId) {
    int storageGroupId = toStorageGroupId(deviceId);

    // check if we record the mapping between device id and storage group id
    if (!sgToDevice[storageGroupId].contains(deviceId)) {
      synchronized (sgToDevice) {
        // double check
        if (!sgToDevice[storageGroupId].add(deviceId)) {
          // add new mapping to file
          // TODO write to file
        }
      }
    }

    try {
      return new PartialPath(String.valueOf(storageGroupId));
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public Set<PartialPath> storageGroupToDevice(PartialPath storageGroup) {
    return sgToDevice[Integer.parseInt(storageGroup.getFullPath())];
  }

  @Override
  public void clear() {
    for (int i = 0; i < STORAGE_GROUP_NUM; i++) {
      sgToDevice[i] = new HashSet<>();
    }
  }

  @Override
  public int getPartitionCount() {
    return STORAGE_GROUP_NUM;
  }

  public void recover() {

  }

  private int toStorageGroupId(PartialPath deviceId) {
    return Math.abs(deviceId.hashCode() % STORAGE_GROUP_NUM);
  }

  private static class HashVirtualPartitionerHolder {

    private static final HashVirtualPartitioner INSTANCE = new HashVirtualPartitioner();

    private HashVirtualPartitionerHolder() {
      // allowed to do nothing
    }
  }
}
