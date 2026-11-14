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

package org.apache.iotdb.consensus.common.request;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Outcomes that may durably advance an applied frontier. Returning an error status is not proof
 * that an application attempt completed: DataRegion also returns local storage/rejection errors.
 * Only success is accepted: even TTL, schema and type errors can depend on replica-local time or
 * metadata. A failed attempt stops the replica so recovery can replay from a clean/checkpointed
 * application after the cause is corrected. This is not an automatic retry loop.
 */
public final class StateMachineApplyResult {
  private StateMachineApplyResult() {}

  public static void requireCompleted(TSStatus status) throws IOException {
    if (!isCompleted(status)) {
      throw new IOException(
          "State machine did not complete committed entry; status="
              + (status == null ? "null" : status.getCode()));
    }
  }

  public static boolean isCompleted(TSStatus status) {
    if (status != null && status.getSubStatus() == null) {
      // The common single status: no children, so no cycle to guard against.
      return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }
    return isCompleted(
        status, Collections.newSetFromMap(new IdentityHashMap<TSStatus, Boolean>()), 0);
  }

  private static boolean isCompleted(TSStatus status, Set<TSStatus> path, int depth) {
    if (status == null || depth >= 64 || !path.add(status)) {
      return false;
    }
    try {
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return false;
      }
      List<TSStatus> children = status.getSubStatus();
      if (children != null) {
        for (TSStatus child : children) {
          if (!isCompleted(child, path, depth + 1)) {
            return false;
          }
        }
      }
      return true;
    } finally {
      path.remove(status);
    }
  }
}
