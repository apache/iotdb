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

public class PipeDeleteLogicalViewPlan extends ConfigPhysicalPlan {

  private ByteBuffer patternTreeBytes;

  public PipeDeleteLogicalViewPlan() {
    super(ConfigPhysicalPlanType.PipeDeleteLogicalView);
  }

  public PipeDeleteLogicalViewPlan(final @Nonnull ByteBuffer patternTreeBytes) {
    super(ConfigPhysicalPlanType.PipeDeleteLogicalView);
    this.patternTreeBytes = patternTreeBytes;
  }

  public ByteBuffer getPatternTreeBytes() {
    patternTreeBytes.rewind();
    return patternTreeBytes;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(patternTreeBytes, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    patternTreeBytes = ByteBuffer.wrap(ReadWriteIOUtils.readBinary(buffer).getValues());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeDeleteLogicalViewPlan that = (PipeDeleteLogicalViewPlan) obj;
    return Arrays.equals(patternTreeBytes.array(), that.patternTreeBytes.array());
  }

  @Override
  public int hashCode() {
    return Objects.hash(patternTreeBytes);
  }

  @Override
  public String toString() {
    return "PipeDeleteLogicalViewPlan{"
        + "patternTreeBytes='"
        + Arrays.toString(patternTreeBytes.array())
        + "'}";
  }
}
