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

package org.apache.iotdb.commons.client.async;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.util.List;
import java.util.Objects;

public class AsyncTEndPoint extends TEndPoint {

  private int minSendPortRange;
  private int maxSendPortRange;
  private List<Integer> candidatePorts;
  private String customSendPortStrategy;

  public AsyncTEndPoint(
      String ip,
      int port,
      int minSendPortRange,
      int maxSendPortRange,
      List<Integer> candidatePorts,
      String customSendPortStrategy) {
    super(ip, port);
    this.minSendPortRange = minSendPortRange;
    this.maxSendPortRange = maxSendPortRange;
    this.candidatePorts = candidatePorts;
    this.customSendPortStrategy = customSendPortStrategy;
  }

  public int getMinSendPortRange() {
    return minSendPortRange;
  }

  public void setMinSendPortRange(int minSendPortRange) {
    this.minSendPortRange = minSendPortRange;
  }

  public int getMaxSendPortRange() {
    return maxSendPortRange;
  }

  public void setMaxSendPortRange(int maxSendPortRange) {
    this.maxSendPortRange = maxSendPortRange;
  }

  public List<Integer> getCandidatePorts() {
    return candidatePorts;
  }

  public void setCandidatePorts(List<Integer> candidatePorts) {
    this.candidatePorts = candidatePorts;
  }

  public String getCustomSendPortStrategy() {
    return customSendPortStrategy;
  }

  public void setCustomSendPortStrategy(String customSendPortStrategy) {
    this.customSendPortStrategy = customSendPortStrategy;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        minSendPortRange,
        maxSendPortRange,
        candidatePorts,
        customSendPortStrategy);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof AsyncTEndPoint)) return false;
    AsyncTEndPoint that = (AsyncTEndPoint) obj;
    return minSendPortRange == that.minSendPortRange
        && maxSendPortRange == that.maxSendPortRange
        && Objects.equals(candidatePorts, that.candidatePorts)
        && Objects.equals(customSendPortStrategy, that.customSendPortStrategy)
        && super.equals(obj);
  }
}
