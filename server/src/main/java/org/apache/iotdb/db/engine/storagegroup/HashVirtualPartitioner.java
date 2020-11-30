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
package org.apache.iotdb.db.engine.storagegroup;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;

public class HashVirtualPartitioner implements VirtualPartitioner {

  public static final int STORGARE_GROUP_NUM = 2;
  HashMap<Integer, Set<PartialPath>> sgToDevice;

  private HashVirtualPartitioner() {
    sgToDevice = new HashMap<>();
  }

  public static HashVirtualPartitioner getInstance() {
    return HashVirtualPartitionerHolder.INSTANCE;
  }

  private int toPartitionId(PartialPath deviceId){
    return deviceId.hashCode() % STORGARE_GROUP_NUM;
  }

  @Override
  public PartialPath deviceToStorageGroup(PartialPath deviceId) {
    int partitionId = toPartitionId(deviceId);
    sgToDevice.computeIfAbsent(partitionId, id -> new HashSet<>()).add(deviceId);
    try {
      return new PartialPath("" + partitionId);
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public Set<PartialPath> storageGroupToDevice(PartialPath storageGroup) {
    return sgToDevice.get(Integer.parseInt(storageGroup.getFullPath()));
  }

  @Override
  public void clear(){
    sgToDevice.clear();
  }

  @Override
  public int getPartitionCount() {
    return STORGARE_GROUP_NUM;
  }

  private static class HashVirtualPartitionerHolder {

    private static final HashVirtualPartitioner INSTANCE = new HashVirtualPartitioner();

    private HashVirtualPartitionerHolder() {
      // allowed to do nothing
    }
  }
}
