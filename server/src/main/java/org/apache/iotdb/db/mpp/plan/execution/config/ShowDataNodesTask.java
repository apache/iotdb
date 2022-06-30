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

package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.common.rpc.thrift.TDataNodesInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class ShowDataNodesTask implements IConfigTask {

  private ShowDataNodesStatement showDataNodesStatement;

  public ShowDataNodesTask() {}

  public ShowDataNodesTask(ShowDataNodesStatement showDataNodesStatement) {
    this.showDataNodesStatement = showDataNodesStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showDataNodes(showDataNodesStatement);
  }

  public static void buildTSBlock(
      TShowDataNodesResp showDataNodesResp, SettableFuture<ConfigTaskResult> future) {
    TsBlockBuilder builder =
        new TsBlockBuilder(HeaderConstant.showDataNodesHeader.getRespDataTypes());
    if (showDataNodesResp.getDataNodesInfoList() != null) {
      for (TDataNodesInfo tDataNodesLocation : showDataNodesResp.getDataNodesInfoList()) {
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeInt(tDataNodesLocation.getDataNodeId());
        builder
            .getColumnBuilder(1)
            .writeBinary(
                Binary.valueOf(
                    tDataNodesLocation.getStatus() == null ? "" : tDataNodesLocation.getStatus()));

        builder
            .getColumnBuilder(2)
            .writeBinary(Binary.valueOf(tDataNodesLocation.getRpcAddresss()));
        builder.getColumnBuilder(3).writeInt(tDataNodesLocation.getRpcPort());
        builder.getColumnBuilder(4).writeInt(tDataNodesLocation.getDataRegionNum());

        builder.getColumnBuilder(5).writeInt(tDataNodesLocation.getSchemaRegionNum());
        builder.declarePosition();
      }
    }
    DatasetHeader datasetHeader = HeaderConstant.showDataNodesHeader;
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
