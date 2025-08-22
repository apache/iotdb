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

package org.apache.iotdb.confignode.consensus.request.write.service;

import org.apache.iotdb.commons.service.external.ServiceInformation;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CreateServicePlan extends ConfigPhysicalPlan {

  private ServiceInformation serviceInformation;
  private Binary jarFile;

  public CreateServicePlan() {
    super(ConfigPhysicalPlanType.CreateService);
  }

  public CreateServicePlan(ServiceInformation serviceInformation, Binary jarFile) {
    super(ConfigPhysicalPlanType.CreateService);
    this.serviceInformation = serviceInformation;
    this.jarFile = jarFile;
  }

  public ServiceInformation getServiceInformation() {
    return serviceInformation;
  }

  public Binary getJarFile() {
    return jarFile;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    serviceInformation.serialize(stream);
    if (jarFile == null || jarFile.getLength() < 0) {
      ReadWriteIOUtils.write(true, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
      ReadWriteIOUtils.write(jarFile, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    serviceInformation = ServiceInformation.deserialize(buffer);
    if (ReadWriteIOUtils.readBool(buffer)) {
      return;
    }
    jarFile = ReadWriteIOUtils.readBinary(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateServicePlan)) {
      return false;
    }
    CreateServicePlan that = (CreateServicePlan) o;
    return serviceInformation.equals(that.serviceInformation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), serviceInformation);
  }
}
