/**
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
package org.apache.iotdb.cluster.entity.raft;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataStateManchine extends StateMachineAdapter {

  // All Storage Groups in Cluster
  private List<String> storageGroupList;

  // Map<username, password>
  Map<String, String> userProfileMap;

  public MetadataStateManchine() {
    storageGroupList = new ArrayList<>();
    userProfileMap = new HashMap<>();
  }

  // Update StrageGroup List and userProfileMap based on Task read from raft log
  @Override
  public void onApply(Iterator iterator) {

  }

  public boolean isStorageGroupLegal(String sg) {
    return storageGroupList.contains(sg);
  }

  public boolean isUerProfileLegal(String username, String password) {
    if (userProfileMap.containsKey(username)) {
      return password.equals(userProfileMap.get(username));
    } else {
      return false;
    }
  }

  public void addStorageGroup(String sg) {
    storageGroupList.add(sg);
  }

  public void deleteStorageGroup(String sg) {
    storageGroupList.remove(sg);
  }

  public void addUser(String username, String password) {
    userProfileMap.put(username, password);
  }

  public void deleteUSer(String username, String password) {
    userProfileMap.remove(username, password);
  }

  public void updateUser(String username, String oldPassword, String newPassword) {
    if (isUerProfileLegal(username, oldPassword)) {
      userProfileMap.put(username, newPassword);
    }
  }
}
