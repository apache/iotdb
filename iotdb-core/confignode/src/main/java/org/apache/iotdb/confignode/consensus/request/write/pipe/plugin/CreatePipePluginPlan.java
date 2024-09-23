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

package org.apache.iotdb.confignode.consensus.request.write.pipe.plugin;

import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CreatePipePluginPlan extends ConfigPhysicalPlan {
  private PipePluginMeta pipePluginMeta;

  private Binary jarFile;

  public CreatePipePluginPlan() {
    super(ConfigPhysicalPlanType.CreatePipePlugin);
  }

  public CreatePipePluginPlan(PipePluginMeta pipePluginMeta, Binary jarFile) {
    super(ConfigPhysicalPlanType.CreatePipePlugin);
    this.pipePluginMeta = pipePluginMeta;
    this.jarFile = jarFile;
  }

  public PipePluginMeta getPipePluginMeta() {
    return pipePluginMeta;
  }

  public Binary getJarFile() {
    return jarFile;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    pipePluginMeta.serialize(stream);
    if (jarFile == null) {
      ReadWriteIOUtils.write(true, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
      ReadWriteIOUtils.write(jarFile, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    pipePluginMeta = PipePluginMeta.deserialize(buffer);
    if (ReadWriteIOUtils.readBool(buffer)) {
      return;
    }
    jarFile = ReadWriteIOUtils.readBinary(buffer);
  }
}
