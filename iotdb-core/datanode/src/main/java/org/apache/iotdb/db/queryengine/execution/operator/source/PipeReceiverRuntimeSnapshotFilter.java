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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.pipe.receiver.runtime.PipeReceiverRuntimeRegistry;
import org.apache.iotdb.commons.pipe.receiver.runtime.PipeReceiverRuntimeSnapshot;
import org.apache.iotdb.db.auth.AuthorityChecker;

import java.util.List;
import java.util.stream.Collectors;

public final class PipeReceiverRuntimeSnapshotFilter {

  private PipeReceiverRuntimeSnapshotFilter() {
    // utility class
  }

  public static List<PipeReceiverRuntimeSnapshot> visibleSnapshots(final UserEntity userEntity) {
    final List<PipeReceiverRuntimeSnapshot> snapshots =
        PipeReceiverRuntimeRegistry.getInstance().snapshot();
    if (canSeeAllReceivers(userEntity)) {
      return snapshots;
    }
    final String userName = userEntity.getUsername();
    return snapshots.stream()
        .filter(snapshot -> userName.equals(snapshot.getUserName()))
        .collect(Collectors.toList());
  }

  private static boolean canSeeAllReceivers(final UserEntity userEntity) {
    if (userEntity == null || userEntity.getUsername() == null) {
      return true;
    }
    if (AuthorityChecker.SUPER_USER_ID == userEntity.getUserId()
        || AuthorityChecker.SUPER_USER.equals(userEntity.getUsername())) {
      return true;
    }
    try {
      return AuthorityChecker.checkSystemPermission(userEntity.getUsername(), PrivilegeType.SYSTEM);
    } catch (final Exception e) {
      return false;
    }
  }
}
