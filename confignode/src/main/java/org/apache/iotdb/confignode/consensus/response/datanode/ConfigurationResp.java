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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TCQConfig;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.confignode.rpc.thrift.TRatisConfig;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

public class ConfigurationResp implements DataSet {

  private TSStatus status;
  private TGlobalConfig globalConfig;
  private TRatisConfig ratisConfig;
  private TCQConfig cqConfig;

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public void setGlobalConfig(TGlobalConfig globalConfig) {
    this.globalConfig = globalConfig;
  }

  public void setRatisConfig(TRatisConfig ratisConfig) {
    this.ratisConfig = ratisConfig;
  }

  public void setCqConfig(TCQConfig cqConfig) {
    this.cqConfig = cqConfig;
  }

  public TSystemConfigurationResp convertToRpcSystemConfigurationResp() {
    TSystemConfigurationResp resp = new TSystemConfigurationResp();
    resp.setStatus(status);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setGlobalConfig(globalConfig);
      resp.setRatisConfig(ratisConfig);
      resp.setCqConfig(cqConfig);
    }
    return resp;
  }
}
