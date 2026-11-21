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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys;

import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesProcedureInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ShowDeleteTimeSeriesProgressTask implements IConfigTask {
  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor executor)
      throws InterruptedException {
    return executor.showDeleteTimeSeriesProgress();
  }

  public static void buildTsBlock(
      List<TDeleteTimeSeriesProcedureInfo> procedures, SettableFuture<ConfigTaskResult> future) {
    final TsBlockBuilder builder =
        new TsBlockBuilder(
            Arrays.asList(
                TSDataType.INT64,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT));
    for (TDeleteTimeSeriesProcedureInfo info :
        procedures == null ? Collections.<TDeleteTimeSeriesProcedureInfo>emptyList() : procedures) {
      builder.getTimeColumnBuilder().writeLong(0);
      builder.getColumnBuilder(0).writeLong(info.getProcedureId());
      builder
          .getColumnBuilder(1)
          .writeBinary(new Binary(info.getQueryId(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(2)
          .writeBinary(new Binary(info.getPathPatterns(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(3)
          .writeBinary(new Binary(info.getState(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(4)
          .writeBinary(new Binary(info.getProgress(), TSFileConfig.STRING_CHARSET));
      if (info.isSetErrorMessage()) {
        builder
            .getColumnBuilder(5)
            .writeBinary(new Binary(info.getErrorMessage(), TSFileConfig.STRING_CHARSET));
      } else {
        builder.getColumnBuilder(5).appendNull();
      }
      builder.declarePosition();
    }
    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            builder.build(),
            DatasetHeaderFactory.getShowDeleteTimeSeriesProgressHeader()));
  }
}
