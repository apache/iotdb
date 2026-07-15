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

package org.apache.iotdb.confignode.consensus.request.write.pipe.payload;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class PipeRenameTimeSeriesPlan extends ConfigPhysicalPlan {

  private ByteBuffer oldPathBytes;
  private ByteBuffer newPathBytes;
  private boolean generatedFromSnapshot;

  public PipeRenameTimeSeriesPlan() {
    super(ConfigPhysicalPlanType.PipeRenameTimeSeries);
  }

  public PipeRenameTimeSeriesPlan(
      final @Nonnull ByteBuffer oldPathBytes, final @Nonnull ByteBuffer newPathBytes) {
    super(ConfigPhysicalPlanType.PipeRenameTimeSeries);
    this.oldPathBytes = oldPathBytes;
    this.newPathBytes = newPathBytes;
  }

  public ByteBuffer getOldPathBytes() {
    oldPathBytes.rewind();
    return oldPathBytes;
  }

  public ByteBuffer getNewPathBytes() {
    newPathBytes.rewind();
    return newPathBytes;
  }

  public boolean isGeneratedFromSnapshot() {
    return generatedFromSnapshot;
  }

  public PipeRenameTimeSeriesPlan markAsGeneratedFromSnapshot() {
    generatedFromSnapshot = true;
    return this;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(oldPathBytes, stream);
    ReadWriteIOUtils.write(newPathBytes, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    oldPathBytes = ByteBuffer.wrap(ReadWriteIOUtils.readBinary(buffer).getValues());
    newPathBytes = ByteBuffer.wrap(ReadWriteIOUtils.readBinary(buffer).getValues());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeRenameTimeSeriesPlan that = (PipeRenameTimeSeriesPlan) obj;
    return Arrays.equals(oldPathBytes.array(), that.oldPathBytes.array())
        && Arrays.equals(newPathBytes.array(), that.newPathBytes.array());
  }

  @Override
  public int hashCode() {
    return Objects.hash(oldPathBytes, newPathBytes);
  }

  @Override
  public String toString() {
    return "PipeRenameTimeSeriesPlan{"
        + "oldPathBytes='"
        + Arrays.toString(oldPathBytes.array())
        + "', newPathBytes='"
        + Arrays.toString(newPathBytes.array())
        + "'}";
  }
}
