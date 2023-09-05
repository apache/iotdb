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

package org.apache.iotdb.confignode.consensus.response.datanode;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Map;

public class DataNodeToStatusResp implements DataSet {
  private TSStatus status;

  private Map<TDataNodeLocation, TSStatus> dataNodeLocationToStatus;

  public DataNodeToStatusResp() {
    // empty constructor
  }

  public TDataNodeRemoveResp convertToRpCDataNodeRemoveResp() {
    TDataNodeRemoveResp resp = new TDataNodeRemoveResp();
    resp.setStatus(status);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setNodeToStatus(dataNodeLocationToStatus);
    }

    return resp;
  }

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public Map<TDataNodeLocation, TSStatus> getDataNodeLocationToStatus() {
    return dataNodeLocationToStatus;
  }

  public void setDataNodeLocationToStatus(
      Map<TDataNodeLocation, TSStatus> dataNodeLocationToStatus) {
    this.dataNodeLocationToStatus = dataNodeLocationToStatus;
  }
}
