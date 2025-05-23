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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDatabaseStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.Collections;

public class CountDatabaseTask implements IConfigTask {

  private final CountDatabaseStatement countDatabaseStatement;

  public CountDatabaseTask(CountDatabaseStatement countDatabaseStatement) {
    this.countDatabaseStatement = countDatabaseStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.countDatabase(countDatabaseStatement);
  }

  public static void buildTSBlock(int storageGroupNum, SettableFuture<ConfigTaskResult> future) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeInt(storageGroupNum);
    builder.declarePosition();
    ColumnHeader storageGroupColumnHeader =
        new ColumnHeader(IoTDBConstant.COLUMN_COUNT, TSDataType.INT32);
    DatasetHeader datasetHeader =
        new DatasetHeader(Collections.singletonList(storageGroupColumnHeader), true);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
