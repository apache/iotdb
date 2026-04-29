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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.externalservice;

import org.apache.iotdb.common.rpc.thrift.TExternalServiceEntry;
import org.apache.iotdb.commons.externalservice.ServiceInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;

public class ShowExternalServiceTask implements IConfigTask {
  private final int dataNodeId;

  public ShowExternalServiceTask(int dataNodeId) {
    this.dataNodeId = dataNodeId;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showExternalService(dataNodeId);
  }

  public static void appendServiceEntry(
      TExternalServiceEntry externalServiceEntry, ColumnBuilder[] columnBuilders) {
    columnBuilders[0].writeBinary(
        new Binary(externalServiceEntry.getServiceName(), TSFileConfig.STRING_CHARSET));
    columnBuilders[1].writeInt(externalServiceEntry.getDataNodeId());
    columnBuilders[2].writeBinary(
        new Binary(
            ServiceInfo.State.deserialize(externalServiceEntry.getState()).toString(),
            TSFileConfig.STRING_CHARSET));
  }
}
