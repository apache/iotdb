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

package com.timecho.iotdb.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.confignode.procedure.consensus.request.write.auth.EnableSeparationOfAdminPowersPlan;
import com.timecho.iotdb.confignode.procedure.impl.auth.EnableSeparationOfAdminPowersProcedure;

public class TimechoProcedureManager extends ProcedureManager {
  public TimechoProcedureManager(ConfigManager configManager, ProcedureInfo procedureInfo) {
    super(configManager, procedureInfo);
  }

  public TSStatus enableSeparationOfAdminPowers(EnableSeparationOfAdminPowersPlan plan) {
    try {
      EnableSeparationOfAdminPowersProcedure procedure =
          new EnableSeparationOfAdminPowersProcedure(plan);
      getExecutor().submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        status.unsetMessage();
      }
      return status;
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.AUTH_OPERATE_EXCEPTION.getStatusCode())
          .setMessage(e.getMessage());
    }
  }
}
