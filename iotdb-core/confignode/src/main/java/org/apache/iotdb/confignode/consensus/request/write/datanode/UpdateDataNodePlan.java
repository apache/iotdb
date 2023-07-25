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

package org.apache.iotdb.confignode.consensus.request.write.datanode;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UpdateDataNodePlan extends ConfigPhysicalPlan {

  private TDataNodeConfiguration dataNodeConfiguration;
  private String buildInfo;

  public UpdateDataNodePlan() {
    super(ConfigPhysicalPlanType.UpdateDataNodeConfiguration);
  }

  public UpdateDataNodePlan(TDataNodeConfiguration dataNodeConfiguration) {
    this();
    this.dataNodeConfiguration = dataNodeConfiguration;
    this.buildInfo = "";
  }

  public TDataNodeConfiguration getDataNodeConfiguration() {
    return dataNodeConfiguration;
  }

  public void setBuildInfo(String buildInfo) {
    this.buildInfo = buildInfo;
  }

  public String getBuildInfo() {
    return buildInfo;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftCommonsSerDeUtils.serializeTDataNodeConfiguration(dataNodeConfiguration, stream);
    ReadWriteIOUtils.write(buildInfo, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    dataNodeConfiguration = ThriftCommonsSerDeUtils.deserializeTDataNodeConfiguration(buffer);
    if (buffer.hasRemaining()) {
      this.buildInfo = ReadWriteIOUtils.readString(buffer);
    } else {
      buildInfo = "";
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
    if (!getType().equals(((UpdateDataNodePlan) o).getType())) {
      return false;
    }
    UpdateDataNodePlan that = (UpdateDataNodePlan) o;
    return dataNodeConfiguration.equals(that.dataNodeConfiguration)
        && buildInfo.equals(that.buildInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), dataNodeConfiguration, buildInfo);
  }
}
