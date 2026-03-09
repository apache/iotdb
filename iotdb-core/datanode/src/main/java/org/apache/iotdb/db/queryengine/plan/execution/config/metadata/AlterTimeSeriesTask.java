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

import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class AlterTimeSeriesTask implements IConfigTask {
  private final String queryId;

  private final AlterTimeSeriesStatement alterTimeSeriesStatement;

  public AlterTimeSeriesTask(
      final String queryId, final AlterTimeSeriesStatement alterTimeSeriesStatement) {
    this.queryId = queryId;
    this.alterTimeSeriesStatement = alterTimeSeriesStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    if (alterTimeSeriesStatement
        .getAlterType()
        .equals(AlterTimeSeriesStatement.AlterType.SET_DATA_TYPE)) {
      if (alterTimeSeriesStatement.getDataType() == null) {
        throw new IllegalArgumentException(
            String.format(
                "Data type cannot be null executing the statement that alter timeseries %s set data type",
                alterTimeSeriesStatement.getPath().getFullPath()));
      }
      return configTaskExecutor.alterTimeSeriesDataType(queryId, alterTimeSeriesStatement);
    } else {
      throw new UnsupportedOperationException("Not support current statement");
    }
  }
}
