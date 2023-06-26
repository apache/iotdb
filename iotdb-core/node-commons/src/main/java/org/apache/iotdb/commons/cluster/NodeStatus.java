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

package org.apache.iotdb.commons.cluster;

/** Node status for showing cluster */
public enum NodeStatus {
  /** Node running properly */
  Running("Running"),

  /** Node connection failure */
  Unknown("Unknown"),

  /** Node is in removing */
  Removing("Removing"),

  /** Only query statements are permitted */
  ReadOnly("ReadOnly");
  public static final String DISK_FULL = "DiskFull";

  private final String status;

  NodeStatus(String status) {
    this.status = status;
  }

  public String getStatus() {
    return status;
  }

  public static NodeStatus parse(String status) {
    for (NodeStatus nodeStatus : NodeStatus.values()) {
      if (nodeStatus.status.equals(status)) {
        return nodeStatus;
      }
    }
    throw new RuntimeException(String.format("NodeStatus %s doesn't exist.", status));
  }

  public static boolean isNormalStatus(NodeStatus status) {
    // Currently, the only normal status is Running
    return status != null && status.equals(NodeStatus.Running);
  }
}
