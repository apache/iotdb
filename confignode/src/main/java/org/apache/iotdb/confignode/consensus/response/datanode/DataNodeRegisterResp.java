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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TRuntimeConfiguration;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DataNodeRegisterResp implements DataSet {

  private TSStatus status;
  private List<TConfigNodeLocation> configNodeList;
  private Integer dataNodeId;

  private TRuntimeConfiguration runtimeConfiguration;

  public DataNodeRegisterResp() {
    this.dataNodeId = null;
  }

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public void setConfigNodeList(List<TConfigNodeLocation> configNodeList) {
    this.configNodeList = configNodeList;
  }

  public void setDataNodeId(Integer dataNodeId) {
    this.dataNodeId = dataNodeId;
  }

  public void setRuntimeConfiguration(TRuntimeConfiguration runtimeConfiguration) {
    this.runtimeConfiguration = runtimeConfiguration;
  }

  public static byte[] convertAllTTLInformation(Map<String, Long> allTTLInformation) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(allTTLInformation.size(), outputStream);
      for (Map.Entry<String, Long> entry : allTTLInformation.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
    } catch (IOException ignored) {
      // Normally, this line will never reach
    }
    return outputStream.toByteArray();
  }

  public TDataNodeRegisterResp convertToRpcDataNodeRegisterResp() {
    TDataNodeRegisterResp resp = new TDataNodeRegisterResp();
    resp.setStatus(status);
    resp.setConfigNodeList(configNodeList);

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setDataNodeId(dataNodeId);
      resp.setRuntimeConfiguration(runtimeConfiguration);
    }

    return resp;
  }
}
