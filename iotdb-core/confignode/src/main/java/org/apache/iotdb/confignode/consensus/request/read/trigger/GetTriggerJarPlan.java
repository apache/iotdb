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

package org.apache.iotdb.confignode.consensus.request.read.trigger;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GetTriggerJarPlan extends ConfigPhysicalPlan {

  private List<String> jarNames;

  public GetTriggerJarPlan() {
    super(ConfigPhysicalPlanType.GetTriggerJar);
  }

  public GetTriggerJarPlan(List<String> triggerNames) {
    super(ConfigPhysicalPlanType.GetTriggerJar);
    jarNames = triggerNames;
  }

  public List<String> getJarNames() {
    return jarNames;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    ReadWriteIOUtils.write(jarNames.size(), stream);
    for (String jarName : jarNames) {
      ReadWriteIOUtils.write(jarName, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    jarNames = new ArrayList<>(size);
    while (size > 0) {
      jarNames.add(ReadWriteIOUtils.readString(buffer));
      size--;
    }
  }
}
