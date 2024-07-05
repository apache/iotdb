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

package org.apache.iotdb.confignode.manager.load.cache.consensus;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.manager.load.cache.AbstractStatistics;

import java.util.Objects;

/** ConsensusGroupStatistics indicates the statistics of a consensus group. */
public class ConsensusGroupStatistics extends AbstractStatistics {

  private final int leaderId;

  public ConsensusGroupStatistics(long statisticsNanoTimestamp, int leaderId) {
    super(statisticsNanoTimestamp);
    this.leaderId = leaderId;
  }

  @TestOnly
  public ConsensusGroupStatistics(int leaderId) {
    super(System.nanoTime());
    this.leaderId = leaderId;
  }

  public static ConsensusGroupStatistics generateDefaultConsensusGroupStatistics() {
    return new ConsensusGroupStatistics(0, ConsensusGroupCache.UN_READY_LEADER_ID);
  }

  public int getLeaderId() {
    return leaderId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConsensusGroupStatistics that = (ConsensusGroupStatistics) o;
    return leaderId == that.leaderId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(leaderId);
  }

  @Override
  public String toString() {
    return "ConsensusGroupStatistics{" + "leaderId=" + leaderId + '}';
  }
}
