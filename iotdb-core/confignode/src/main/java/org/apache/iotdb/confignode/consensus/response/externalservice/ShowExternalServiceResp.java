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
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import javax.validation.constraints.NotNull;

import java.util.Comparator;
import java.util.List;

public class ShowExternalServiceResp implements DataSet {

  private final List<TExternalServiceEntry> serviceInfoEntryList;

  public ShowExternalServiceResp(@NotNull List<TExternalServiceEntry> serviceInfoEntryList) {
    this.serviceInfoEntryList = serviceInfoEntryList;
  }

  public List<TExternalServiceEntry> getServiceInfoEntryList() {
    return serviceInfoEntryList;
  }

  public TExternalServiceListResp convertToRpcShowExternalServiceResp() {
    serviceInfoEntryList.sort(
        Comparator.comparingInt(TExternalServiceEntry::getDataNodeId)
            .thenComparing(TExternalServiceEntry::getServiceType)
            .thenComparing(TExternalServiceEntry::getServiceName));
    return new TExternalServiceListResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), serviceInfoEntryList);
  }
}
