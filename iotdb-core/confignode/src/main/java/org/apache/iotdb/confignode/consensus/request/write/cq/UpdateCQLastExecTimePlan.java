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
import org.apache.iotdb.confignode.i18n.ManagerMessages;

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

  private boolean hasOccurrenceIndex;
  private long expectedIndex;
  private long targetIndex;

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

  public UpdateCQLastExecTimePlan(
      String cqId, long executionTime, String cqToken, long expectedIndex, long targetIndex) {
    this(cqId, executionTime, cqToken);
    if (expectedIndex < 0 || targetIndex <= expectedIndex) {
      throw new IllegalArgumentException(
          ManagerMessages.EXCEPTION_INVALID_CQ_OCCURRENCE_INDEX_TRANSITION_AC6BFC4D);
    }
    this.hasOccurrenceIndex = true;
    this.expectedIndex = expectedIndex;
    this.targetIndex = targetIndex;
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

  public boolean hasOccurrenceIndex() {
    return hasOccurrenceIndex;
  }

  public long getExpectedIndex() {
    return expectedIndex;
  }

  public long getTargetIndex() {
    return targetIndex;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(cqId, stream);
    ReadWriteIOUtils.write(executionTime, stream);
    ReadWriteIOUtils.write(cqToken, stream);
    ReadWriteIOUtils.write(hasOccurrenceIndex, stream);
    if (hasOccurrenceIndex) {
      ReadWriteIOUtils.write(expectedIndex, stream);
      ReadWriteIOUtils.write(targetIndex, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    cqId = ReadWriteIOUtils.readString(buffer);
    executionTime = ReadWriteIOUtils.readLong(buffer);
    cqToken = ReadWriteIOUtils.readString(buffer);
    if (buffer.hasRemaining()) {
      hasOccurrenceIndex = ReadWriteIOUtils.readBool(buffer);
      if (hasOccurrenceIndex) {
        expectedIndex = ReadWriteIOUtils.readLong(buffer);
        targetIndex = ReadWriteIOUtils.readLong(buffer);
      }
    }
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
        && hasOccurrenceIndex == that.hasOccurrenceIndex
        && expectedIndex == that.expectedIndex
        && targetIndex == that.targetIndex
        && cqId.equals(that.cqId)
        && cqToken.equals(that.cqToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        cqId,
        executionTime,
        cqToken,
        hasOccurrenceIndex,
        expectedIndex,
        targetIndex);
  }
}
