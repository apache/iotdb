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


import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.metadata.PartialPath;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaSimpleSnapshotTest {

  private static final Logger logger = LoggerFactory.getLogger(MetaSimpleSnapshotTest.class);

  @Test
  public void testSerialize() {
    try {
      Map<PartialPath, Long> storageGroupTTLMap = new HashMap<>();
      Map<String, User> userMap = new HashMap<>();
      Map<String, Role> roleMap = new HashMap<>();
      PartitionTable partitionTable = TestUtils.getPartitionTable(10);
      long lastLogIndex = 10;
      long lastLogTerm = 5;

      for (int i = 0; i < 10; i++) {
        PartialPath partialPath = new PartialPath("root.ln.sg1");
        storageGroupTTLMap.put(partialPath, (long) i);
      }

      for (int i = 0; i < 5; i++) {
        String userName = "user_" + i;
        User user = new User(userName, "password_" + i);
        userMap.put(userName, user);
      }

      for (int i = 0; i < 10; i++) {
        String roleName = "role_" + i;
        Role role = new Role(roleName);
        roleMap.put(roleName, role);
      }

      MetaSimpleSnapshot metaSimpleSnapshot = new MetaSimpleSnapshot(storageGroupTTLMap, userMap,
          roleMap, partitionTable.serialize());

      metaSimpleSnapshot.setLastLogIndex(lastLogIndex);
      metaSimpleSnapshot.setLastLogTerm(lastLogTerm);

      ByteBuffer buffer = metaSimpleSnapshot.serialize();

      MetaSimpleSnapshot newSnapshot = new MetaSimpleSnapshot();
      newSnapshot.deserialize(buffer);

      Assert.assertEquals(storageGroupTTLMap, newSnapshot.getStorageGroupTTLMap());
      Assert.assertEquals(userMap, newSnapshot.getUserMap());
      Assert.assertEquals(roleMap, newSnapshot.getRoleMap());

      Assert.assertEquals(partitionTable.serialize(), newSnapshot.getPartitionTableBuffer());
      Assert.assertEquals(lastLogIndex, newSnapshot.getLastLogIndex());
      Assert.assertEquals(lastLogTerm, newSnapshot.getLastLogTerm());

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}

