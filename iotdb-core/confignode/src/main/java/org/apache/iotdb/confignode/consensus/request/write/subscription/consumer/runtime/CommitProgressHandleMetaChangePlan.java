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

package org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.runtime;

import org.apache.iotdb.commons.subscription.meta.consumer.CommitProgressKeeper;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Consensus plan for handling commit progress meta changes. Carries a map of commit progress
 * entries collected from DataNodes.
 */
public class CommitProgressHandleMetaChangePlan extends ConfigPhysicalPlan {

  private Map<String, Long> commitProgressMap = new HashMap<>();

  public CommitProgressHandleMetaChangePlan() {
    super(ConfigPhysicalPlanType.CommitProgressHandleMetaChange);
  }

  public CommitProgressHandleMetaChangePlan(final Map<String, Long> commitProgressMap) {
    super(ConfigPhysicalPlanType.CommitProgressHandleMetaChange);
    this.commitProgressMap = commitProgressMap;
  }

  public Map<String, Long> getCommitProgressMap() {
    return commitProgressMap;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    stream.writeInt(commitProgressMap.size());
    for (Map.Entry<String, Long> entry : commitProgressMap.entrySet()) {
      final byte[] keyBytes = entry.getKey().getBytes("UTF-8");
      stream.writeInt(keyBytes.length);
      stream.write(keyBytes);
      stream.writeLong(entry.getValue());
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    commitProgressMap = CommitProgressKeeper.deserializeFromBuffer(buffer);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CommitProgressHandleMetaChangePlan that = (CommitProgressHandleMetaChangePlan) obj;
    return Objects.equals(this.commitProgressMap, that.commitProgressMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitProgressMap);
  }
}
