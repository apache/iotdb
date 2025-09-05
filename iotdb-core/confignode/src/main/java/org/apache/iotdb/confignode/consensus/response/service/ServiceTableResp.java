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

package org.apache.iotdb.confignode.consensus.response.service;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.service.external.ServiceInformation;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.rpc.thrift.TGetServiceTableResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ServiceTableResp implements DataSet {
  private final TSStatus status;
  private final List<ServiceInformation> serviceInfoList;

  public ServiceTableResp(TSStatus status, List<ServiceInformation> serviceInfoList) {
    this.status = status;
    this.serviceInfoList = serviceInfoList;
  }

  public TGetServiceTableResp convertToThriftResponse() throws IOException {
    List<ByteBuffer> serviceTableList = new ArrayList<>();
    for (ServiceInformation serviceInfo : serviceInfoList) {
      serviceTableList.add(serviceInfo.serialize());
    }
    return new TGetServiceTableResp(status, serviceTableList);
  }

  @TestOnly
  public TSStatus getStatus() {
    return status;
  }

  @TestOnly
  public List<ServiceInformation> getServiceInfoList() {
    return serviceInfoList;
  }
}
