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
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * This plan is the transmitted version of {@link UnsetSchemaTemplatePlan} to carry adequate message
 * to the receiver. Note that this plan will not be executed by the consensus layer directly.
 */
public class PipeUnsetSchemaTemplatePlan extends ConfigPhysicalPlan {
  private String name;
  private String path;

  public PipeUnsetSchemaTemplatePlan() {
    super(ConfigPhysicalPlanType.PipeUnsetTemplate);
  }

  public PipeUnsetSchemaTemplatePlan(String name, String path) {
    super(ConfigPhysicalPlanType.PipeUnsetTemplate);
    this.name = name;
    this.path = path;
  }

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(name, stream);
    ReadWriteIOUtils.write(path, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    name = ReadWriteIOUtils.readString(buffer);
    path = ReadWriteIOUtils.readString(buffer);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeUnsetSchemaTemplatePlan that = (PipeUnsetSchemaTemplatePlan) obj;
    return name.equals(that.name) && path.equals(that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, path);
  }

  @Override
  public String toString() {
    return "PipeUnsetSchemaTemplatePlan{" + "name='" + name + "'path='" + path + "'}";
  }
}
