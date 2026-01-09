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

package org.apache.iotdb.confignode.consensus.response.externalservice;

import org.apache.iotdb.common.rpc.thrift.TExternalServiceEntry;
import org.apache.iotdb.common.rpc.thrift.TExternalServiceListResp;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.externalservice.ServiceInfo;
import org.apache.iotdb.consensus.common.DataSet;

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.stream.Collectors;

public class ShowExternalServiceResp implements DataSet {

  private final TSStatus status;
  private final List<ServiceInfo> serviceInfos;

  public ShowExternalServiceResp(
      @NotNull TSStatus status, @NotNull List<ServiceInfo> serviceInfos) {
    this.status = status;
    this.serviceInfos = serviceInfos;
  }

  public TExternalServiceListResp convertToRpcShowExternalServiceResp() {
    return new TExternalServiceListResp(
        status,
        serviceInfos.stream()
            .map(
                entry ->
                    new TExternalServiceEntry(
                        entry.getServiceName(), entry.getClassName(), entry.getState().getValue()))
            .collect(Collectors.toList()));
  }
}
