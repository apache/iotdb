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

package org.apache.iotdb.confignode.consensus.request.write.pipe.runtime;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PipeHandleMetaChangePlan extends ConfigPhysicalPlan {

  private List<PipeMeta> pipeMetaList = new ArrayList<>();

  public PipeHandleMetaChangePlan() {
    super(ConfigPhysicalPlanType.PipeHandleMetaChange);
  }

  public PipeHandleMetaChangePlan(List<PipeMeta> pipeMetaList) {
    super(ConfigPhysicalPlanType.PipeHandleMetaChange);
    this.pipeMetaList = pipeMetaList;
  }

  public List<PipeMeta> getPipeMetaList() {
    return pipeMetaList;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(pipeMetaList.size());
    for (PipeMeta pipeMeta : pipeMetaList) {
      pipeMeta.serialize(stream);
    }
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      PipeMeta pipeMeta = PipeMeta.deserialize4Coordinator(buffer);
      pipeMetaList.add(pipeMeta);
    }
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeHandleMetaChangePlan that = (PipeHandleMetaChangePlan) obj;
    return Objects.equals(this.pipeMetaList, that.pipeMetaList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeMetaList);
  }
}
