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
  Running("Running", 1),

  /**
   * For strong consistency algorithms, the RegionGroup is considered as Available when the number
   * of Regions in the Running status is greater than half. For weak consistency algorithms, the
   * RegionGroup is considered as Available when the number of Regions in the Running status is
   * greater than or equal to 1. To avoid the impact of Removing and Adding region status on region
   * group status evaluation, this status, which only occurs during region migration and
   * reconstruction, can be excluded. The denominator uses the number of regions excluding Removing
   * and Adding status, while the numerator uses regions in the Running status, ensuring high
   * availability evaluation remains unaffected.
   */
  Available("Available", 2),

  /** In scenarios other than the two mentioned above. */
  Disabled("Disabled", 3);

  private final String status;
  private final int weight;

  RegionGroupStatus(String status, int weight) {
    this.status = status;
    this.weight = weight;
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

  /**
   * Compare the weight of two RegionGroupStatus.
   *
   * <p>Running > Available > Discouraged > Disabled
   */
  public int compare(RegionGroupStatus other) {
    return Integer.compare(this.weight, other.weight);
  }
}
