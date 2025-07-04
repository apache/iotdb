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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TProcedureInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowProceduresResp;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.BytesUtils;

import java.util.List;
import java.util.stream.Collectors;

public class ShowProcedureTask implements IConfigTask {

  public ShowProcedureTask() {
    // do nothing
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showProcedures();
  }

  public static void buildTsBlock(
      final TShowProceduresResp showProceduresResp, final SettableFuture<ConfigTaskResult> future) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showProceduresColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    if (showProceduresResp.getProceduresInfoList() != null) {
      for (final TProcedureInfo procedureInfo : showProceduresResp.getProceduresInfoList()) {
        builder.getTimeColumnBuilder().writeLong(0);
        builder.getColumnBuilder(0).writeLong(procedureInfo.getProcId());
        builder.getColumnBuilder(1).writeBinary(BytesUtils.valueOf(procedureInfo.getState()));
        builder
            .getColumnBuilder(2)
            .writeBinary(
                BytesUtils.valueOf(
                    DateTimeUtils.convertLongToDate(procedureInfo.getSubmittedTime())));
        builder
            .getColumnBuilder(3)
            .writeBinary(
                BytesUtils.valueOf(DateTimeUtils.convertLongToDate(procedureInfo.getLastUpdate())));
        builder.getColumnBuilder(4).writeLong(procedureInfo.getParentProcId());
        builder.declarePosition();
      }
    }
    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowProceduresHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
