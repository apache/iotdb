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

package org.apache.iotdb.commons.consensus.iotv2.consistency.repair;

/** Actions that the RepairConflictResolver can determine for a single diff entry. */
public enum RepairAction {
  /** Send the Leader's data to the Follower (insert/update). */
  SEND_TO_FOLLOWER,
  /** Send the Follower's data to the Leader (insert/update). */
  SEND_TO_LEADER,
  /** Delete the data on the Follower. */
  DELETE_ON_FOLLOWER,
  /** Delete the data on the Leader. */
  DELETE_ON_LEADER,
  /** Keep the Follower's version (it's newer or more authoritative). */
  KEEP_FOLLOWER,
  /** Skip this entry (no action needed, e.g., Follower's deletion is newer). */
  SKIP,
  /** Skip and raise an alert (anomalous state, e.g., data exists on Follower with no deletion). */
  SKIP_AND_ALERT
}
