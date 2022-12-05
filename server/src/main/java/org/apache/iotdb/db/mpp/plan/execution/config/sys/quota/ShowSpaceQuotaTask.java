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

package org.apache.iotdb.db.mpp.plan.execution.config.sys.quota;

import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.commons.enums.SpaceQuotaType;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.sys.quota.ShowSpaceQuotaStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowSpaceQuotaTask implements IConfigTask {

  private final ShowSpaceQuotaStatement showSpaceQuotaStatement;

  public ShowSpaceQuotaTask(ShowSpaceQuotaStatement showSpaceQuotaStatement) {
    this.showSpaceQuotaStatement = showSpaceQuotaStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showSpaceQuota(showSpaceQuotaStatement);
  }

  public static void buildTSBlock(TSpaceQuotaResp resp, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showSpaceQuotaColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    if (resp.getSpaceQuota() != null) {
      for (Map.Entry<String, TSpaceQuota> spaceQuotaEntry : resp.getSpaceQuota().entrySet()) {
        if (spaceQuotaEntry.getValue().getDiskSize() != -1) {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(Binary.valueOf(spaceQuotaEntry.getKey()));
          builder.getColumnBuilder(1).writeBinary(Binary.valueOf(SpaceQuotaType.diskSize.name()));
          builder
              .getColumnBuilder(2)
              .writeBinary(
                  Binary.valueOf(
                      spaceQuotaEntry.getValue().getDiskSize() == 0
                          ? "unlimited"
                          : spaceQuotaEntry.getValue().getDiskSize() + "M"));
          builder
              .getColumnBuilder(3)
              .writeBinary(
                  Binary.valueOf(
                      resp.getUseSpaceQuota().get(spaceQuotaEntry.getKey()).getDiskSize() / 1024
                          + "M"));
          builder.declarePosition();
        }
        if (spaceQuotaEntry.getValue().getDeviceNum() != -1) {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(Binary.valueOf(spaceQuotaEntry.getKey()));
          builder.getColumnBuilder(1).writeBinary(Binary.valueOf(SpaceQuotaType.deviceNum.name()));
          builder
              .getColumnBuilder(2)
              .writeBinary(
                  Binary.valueOf(
                      spaceQuotaEntry.getValue().getDeviceNum() == 0
                          ? "unlimited"
                          : spaceQuotaEntry.getValue().getDeviceNum() + ""));
          builder
              .getColumnBuilder(3)
              .writeBinary(
                  Binary.valueOf(
                      resp.getUseSpaceQuota().get(spaceQuotaEntry.getKey()).getDeviceNum() + ""));
          builder.declarePosition();
        }
        if (spaceQuotaEntry.getValue().getTimeserieNum() != -1) {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(Binary.valueOf(spaceQuotaEntry.getKey()));
          builder
              .getColumnBuilder(1)
              .writeBinary(Binary.valueOf(SpaceQuotaType.timeSeriesNum.name()));
          builder
              .getColumnBuilder(2)
              .writeBinary(
                  Binary.valueOf(
                      spaceQuotaEntry.getValue().getTimeserieNum() == 0
                          ? "unlimited"
                          : spaceQuotaEntry.getValue().getTimeserieNum() + ""));
          builder
              .getColumnBuilder(3)
              .writeBinary(
                  Binary.valueOf(
                      resp.getUseSpaceQuota().get(spaceQuotaEntry.getKey()).getTimeserieNum()
                          + ""));
          builder.declarePosition();
        }
      }
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowSpaceQuotaHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
