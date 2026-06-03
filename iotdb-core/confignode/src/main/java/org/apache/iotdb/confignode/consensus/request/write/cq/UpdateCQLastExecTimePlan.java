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

package org.apache.iotdb.confignode.consensus.request.write.cq;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;

import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType.UPDATE_CQ_LAST_EXEC_TIME;

public class UpdateCQLastExecTimePlan extends ConfigPhysicalPlan {

  private String cqId;

  private long executionTime;

  private String cqToken;

  public UpdateCQLastExecTimePlan() {
    super(UPDATE_CQ_LAST_EXEC_TIME);
  }

  public UpdateCQLastExecTimePlan(String cqId, long executionTime, String cqToken) {
    super(UPDATE_CQ_LAST_EXEC_TIME);
    Validate.notNull(cqId);
    Validate.notNull(cqToken);
    this.cqId = cqId;
    this.executionTime = executionTime;
    this.cqToken = cqToken;
  }

  public String getCqId() {
    return cqId;
  }

  public long getExecutionTime() {
    return executionTime;
  }

  public String getCqToken() {
    return cqToken;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(cqId, stream);
    ReadWriteIOUtils.write(executionTime, stream);
    ReadWriteIOUtils.write(cqToken, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    cqId = ReadWriteIOUtils.readString(buffer);
    executionTime = ReadWriteIOUtils.readLong(buffer);
    cqToken = ReadWriteIOUtils.readString(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    UpdateCQLastExecTimePlan that = (UpdateCQLastExecTimePlan) o;
    return executionTime == that.executionTime
        && cqId.equals(that.cqId)
        && cqToken.equals(that.cqToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), cqId, executionTime, cqToken);
  }
}
