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
package org.apache.iotdb.confignode.consensus.response;

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeMessage;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeMessageResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataNodesInfoDataSet implements DataSet {

  private TSStatus status;
  private List<DataNodeLocation> dataNodeList;

  public DataNodesInfoDataSet() {
    // empty constructor
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public TSStatus getStatus() {
    return status;
  }

  public void setDataNodeList(List<DataNodeLocation> dataNodeList) {
    this.dataNodeList = dataNodeList;
  }

  public List<DataNodeLocation> getDataNodeList() {
    return this.dataNodeList;
  }

  public void convertToRPCDataNodeMessageResp(TDataNodeMessageResp resp) {
    resp.setStatus(status);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      Map<Integer, TDataNodeMessage> msgMap = new HashMap<>();
      for (DataNodeLocation info : dataNodeList) {
        msgMap.put(
            info.getDataNodeId(),
            new TDataNodeMessage(
                info.getDataNodeId(),
                new EndPoint(info.getEndPoint().getIp(), info.getEndPoint().getPort())));
        resp.setDataNodeMessageMap(msgMap);
      }
    }
  }
}
