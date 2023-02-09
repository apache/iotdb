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

package org.apache.iotdb.confignode.consensus.response.cq;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.persistence.cq.CQInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCQEntry;
import org.apache.iotdb.confignode.rpc.thrift.TShowCQResp;
import org.apache.iotdb.consensus.common.DataSet;

import javax.validation.constraints.NotNull;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ShowCQResp implements DataSet {

  private final TSStatus status;
  private final List<CQInfo.CQEntry> cqList;

  public ShowCQResp(@NotNull TSStatus status, @NotNull List<CQInfo.CQEntry> cqList) {
    this.status = status;
    this.cqList = cqList;
  }

  public TShowCQResp convertToRpcShowCQResp() {
    return new TShowCQResp(
        status,
        cqList.stream()
            .map(entry -> new TCQEntry(entry.getCqId(), entry.getSql(), entry.getState().getType()))
            .sorted(Comparator.comparing(entry -> entry.cqId))
            .collect(Collectors.toList()));
  }

  public List<CQInfo.CQEntry> getCqList() {
    return cqList;
  }
}
