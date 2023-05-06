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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.quotas.SpaceQuotaType;
import org.apache.iotdb.confignode.rpc.thrift.TShowSpaceResourceResp;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.sys.quota.ShowSpaceResourceStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowSpaceResourceTask implements IConfigTask {

  private ShowSpaceResourceStatement showSpaceResourceStatement;

  public ShowSpaceResourceTask(ShowSpaceResourceStatement showSpaceResourceStatement) {
    this.showSpaceResourceStatement = showSpaceResourceStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showSpaceResource(showSpaceResourceStatement);
  }

  public static void buildTSBlock(
      TShowSpaceResourceResp resp, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showSpaceResourceColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    // deviceResource
    Map<String, Long> deviceResource = resp.getSpaceResource().get(SpaceQuotaType.deviceNum.name());
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(Binary.valueOf(SpaceQuotaType.deviceNum.name()));
    builder
        .getColumnBuilder(1)
        .writeBinary(Binary.valueOf(deviceResource.get(IoTDBConstant.TOTAL).toString()));
    builder.getColumnBuilder(2).writeBinary(Binary.valueOf(""));
    builder
        .getColumnBuilder(3)
        .writeBinary(Binary.valueOf(deviceResource.get(IoTDBConstant.ALLOCATED).toString()));
    builder
        .getColumnBuilder(4)
        .writeBinary(Binary.valueOf(deviceResource.get(IoTDBConstant.AVAILABLE).toString()));
    builder.getColumnBuilder(5).writeBinary(Binary.valueOf(""));
    builder.declarePosition();

    // timeSeriesResource
    Map<String, Long> timeSeriesResource =
        resp.getSpaceResource().get(SpaceQuotaType.timeSeriesNum.name());
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(Binary.valueOf(SpaceQuotaType.timeSeriesNum.name()));
    builder
        .getColumnBuilder(1)
        .writeBinary(Binary.valueOf(timeSeriesResource.get(IoTDBConstant.TOTAL).toString()));
    builder.getColumnBuilder(2).writeBinary(Binary.valueOf(""));
    builder
        .getColumnBuilder(3)
        .writeBinary(Binary.valueOf(timeSeriesResource.get(IoTDBConstant.ALLOCATED).toString()));
    builder
        .getColumnBuilder(4)
        .writeBinary(Binary.valueOf(timeSeriesResource.get(IoTDBConstant.AVAILABLE).toString()));
    builder.getColumnBuilder(5).writeBinary(Binary.valueOf(""));
    builder.declarePosition();

    // diskResource
    Map<String, Long> diskResource = resp.getSpaceResource().get(SpaceQuotaType.diskSize.name());
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(Binary.valueOf(SpaceQuotaType.diskSize.name()));
    builder
        .getColumnBuilder(1)
        .writeBinary(Binary.valueOf(diskResource.get(IoTDBConstant.TOTAL).toString() + "G"));
    builder
        .getColumnBuilder(2)
        .writeBinary(Binary.valueOf(diskResource.get(IoTDBConstant.NON_USED).toString() + "G"));
    builder
        .getColumnBuilder(3)
        .writeBinary(Binary.valueOf(diskResource.get(IoTDBConstant.ALLOCATED).toString() + "G"));
    builder
        .getColumnBuilder(4)
        .writeBinary(Binary.valueOf(diskResource.get(IoTDBConstant.AVAILABLE).toString() + "G"));
    builder
        .getColumnBuilder(5)
        .writeBinary(Binary.valueOf(diskResource.get(IoTDBConstant.USED).toString() + "G"));
    builder.declarePosition();

    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowSpaceResourceHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
