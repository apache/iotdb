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

package org.apache.iotdb.db.queryengine.plan.execution.config.session;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
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

import java.util.List;
import java.util.stream.Collectors;

public class ShowCurrentSqlDialectTask implements IConfigTask {
  private final String sqlDialect;

  public ShowCurrentSqlDialectTask(String sqlDialect) {
    this.sqlDialect = sqlDialect;
  }

  public static void buildTsBlock(String sqlDialect, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.SHOW_CURRENT_SQL_DIALECT_COLUMN_HEADERS.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder
        .getColumnBuilder(0)
        .writeBinary(new Binary(sqlDialect, TSFileConfig.STRING_CHARSET));
    tsBlockBuilder.declarePosition();

    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            tsBlockBuilder.build(),
            DatasetHeaderFactory.getShowCurrentSqlDialectHeader()));
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showCurrentSqlDialect(sqlDialect);
  }
}
