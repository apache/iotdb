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

package org.apache.iotdb.confignode.manager.node;

/** Safety rules shared by ConfigNode and DataNode removal validation. */
public final class NodeRemovalSafety {

  private static final int WEAK_CONSISTENCY_MULTIPLIER = 1;
  private static final int STRONG_CONSISTENCY_MULTIPLIER = 2;

  private NodeRemovalSafety() {}

  public static boolean isSafe(
      int removingReplicaCount, int replicaCount, boolean strongConsistency) {
    return (long) getSafetyMultiplier(strongConsistency) * removingReplicaCount < replicaCount;
  }

  public static int getSafetyMultiplier(boolean strongConsistency) {
    return strongConsistency ? STRONG_CONSISTENCY_MULTIPLIER : WEAK_CONSISTENCY_MULTIPLIER;
  }
}
