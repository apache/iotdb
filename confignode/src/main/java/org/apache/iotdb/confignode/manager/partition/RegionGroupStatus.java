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
package org.apache.iotdb.confignode.manager.partition;

public enum RegionGroupStatus {

  /** All Regions in RegionGroup are in the Running status */
  Running("Running"),

  /**
   * All Regions in RegionGroup are in the Running or Unknown status, and the number of Regions in
   * the Unknown status is less than half
   */
  Available("Available"),

  /**
   * All Regions in RegionGroup are in the Running, Unknown or ReadOnly status, and at least 1 node
   * is in ReadOnly status, the number of Regions in the Unknown or ReadOnly status is less than
   * half
   */
  Discouraged("Discouraged"),

  /**
   * The following cases will lead to Disabled RegionGroup:
   *
   * <p>1. There is a Region in Removing status
   *
   * <p>2. More than half of the Regions are in Unknown or ReadOnly status
   */
  Disabled("Disabled");

  private final String status;

  RegionGroupStatus(String status) {
    this.status = status;
  }

  public String getStatus() {
    return status;
  }

  public static RegionGroupStatus parse(String status) {
    for (RegionGroupStatus regionGroupStatus : RegionGroupStatus.values()) {
      if (regionGroupStatus.status.equals(status)) {
        return regionGroupStatus;
      }
    }
    throw new RuntimeException(String.format("RegionGroupStatus %s doesn't exist.", status));
  }
}
