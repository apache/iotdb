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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.service;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.service.external.ServiceInformation;
import org.apache.iotdb.confignode.rpc.thrift.TShowServiceInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.BytesUtils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class ShowServicesTask implements IConfigTask {
  private final String serviceName;

  public ShowServicesTask(String serviceName) {
    this.serviceName = serviceName;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showServices(serviceName);
  }

  public static void buildTsBlockForService(
      List<TShowServiceInfo> serviceInformations, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showServiceWithNameColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    if (serviceInformations != null && !serviceInformations.isEmpty()) {
      for (TShowServiceInfo serviceInformation : serviceInformations) {

        builder.getTimeColumnBuilder().writeLong(0L);
        builder
            .getColumnBuilder(0)
            .writeBinary(BytesUtils.valueOf(serviceInformation.getServiceName()));
        builder
            .getColumnBuilder(1)
            .writeBinary(BytesUtils.valueOf(serviceInformation.getClassName()));
        builder.getColumnBuilder(2).writeBinary(BytesUtils.valueOf(serviceInformation.getNodeId()));
        builder.getColumnBuilder(3).writeBinary(BytesUtils.valueOf(serviceInformation.getState()));
        builder
            .getColumnBuilder(4)
            .writeBinary(BytesUtils.valueOf(String.valueOf(serviceInformation.getMessage())));
        builder.declarePosition();
      }
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowServiceHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  public static void buildTsBlockForAllServices(
      List<ByteBuffer> serviceInformations, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showAllServicesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    if (serviceInformations != null && !serviceInformations.isEmpty()) {
      for (ByteBuffer serviceInformationByteBuffer : serviceInformations) {
        ServiceInformation serviceInformation =
            ServiceInformation.deserialize(serviceInformationByteBuffer);
        builder.getTimeColumnBuilder().writeLong(0L);
        builder
            .getColumnBuilder(0)
            .writeBinary(BytesUtils.valueOf(serviceInformation.getServiceName()));
        builder
            .getColumnBuilder(1)
            .writeBinary(BytesUtils.valueOf(serviceInformation.getClassName()));
        builder
            .getColumnBuilder(2)
            .writeBinary(BytesUtils.valueOf(String.valueOf(serviceInformation.getServiceStatus())));
        builder.declarePosition();
      }
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowAllServicesHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
