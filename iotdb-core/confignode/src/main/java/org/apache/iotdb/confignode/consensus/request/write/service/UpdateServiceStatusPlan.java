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

import org.apache.iotdb.commons.service.external.ServiceStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class UpdateServiceStatusPlan extends ConfigPhysicalPlan {

  private String serviceName;
  private ServiceStatus serviceStatus;

  public UpdateServiceStatusPlan() {
    super(ConfigPhysicalPlanType.UpdateService);
  }

  public UpdateServiceStatusPlan(String serviceName, ServiceStatus serviceStatus) {
    super(ConfigPhysicalPlanType.UpdateService);
    this.serviceName = serviceName;
    this.serviceStatus = serviceStatus;
  }

  public String getServiceName() {
    return serviceName;
  }

  public ServiceStatus getServiceStatus() {
    return serviceStatus;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    ReadWriteIOUtils.write(serviceName, stream);
    ReadWriteIOUtils.write(serviceStatus.toString(), stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    serviceName = ReadWriteIOUtils.readString(buffer);
    serviceStatus = ServiceStatus.valueOf(ReadWriteIOUtils.readString(buffer));
  }
}
