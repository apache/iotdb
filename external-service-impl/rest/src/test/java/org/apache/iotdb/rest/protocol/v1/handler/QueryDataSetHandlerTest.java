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

package org.apache.iotdb.rest.protocol.v1.handler;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.rest.protocol.model.ExecutionStatus;
import org.apache.iotdb.rest.protocol.v1.model.QueryDataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryDataSetHandlerTest {

  @Test
  public void fillDataSetWithTimestampsShouldAllowResultAtExactLimit() throws Exception {
    Response response =
        QueryDataSetHandler.fillDataSetWithTimestamps(
            new TestQueryExecution(newTsBlock(1L, 11L, 2L, 22L)), 2, 1);

    assertTrue(response.getEntity() instanceof QueryDataSet);
    QueryDataSet dataSet = (QueryDataSet) response.getEntity();
    assertEquals(Arrays.asList(1L, 2L), dataSet.getTimestamps());
    assertEquals(Collections.singletonList(Arrays.asList(11L, 22L)), dataSet.getValues());
  }

  @Test
  public void fillDataSetWithTimestampsShouldRejectRowsBeyondLimit() throws Exception {
    Response response =
        QueryDataSetHandler.fillDataSetWithTimestamps(
            new TestQueryExecution(newTsBlock(1L, 11L, 2L, 22L)), 1, 1);

    assertTrue(response.getEntity() instanceof ExecutionStatus);
    ExecutionStatus status = (ExecutionStatus) response.getEntity();
    assertEquals(TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode(), status.getCode().intValue());
  }

  private static TsBlock newTsBlock(long... timeAndValues) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    for (int i = 0; i < timeAndValues.length; i += 2) {
      builder.getTimeColumnBuilder().writeLong(timeAndValues[i]);
      builder.getColumnBuilder(0).writeLong(timeAndValues[i + 1]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static final class TestQueryExecution implements IQueryExecution {

    private static final String COLUMN_NAME = "root.sg.d1.s1";

    private final Queue<Optional<TsBlock>> batches = new ArrayDeque<>();
    private final DatasetHeader datasetHeader;

    private TestQueryExecution(TsBlock... tsBlocks) {
      for (TsBlock tsBlock : tsBlocks) {
        batches.add(Optional.of(tsBlock));
      }
      batches.add(Optional.empty());

      datasetHeader =
          new DatasetHeader(
              Collections.singletonList(new ColumnHeader(COLUMN_NAME, TSDataType.INT64)), false);
      datasetHeader.setTreeColumnToTsBlockIndexMap(Collections.singletonList(COLUMN_NAME));
    }

    @Override
    public void start() {}

    @Override
    public void stop(Throwable t) {}

    @Override
    public void stopAndCleanup(Throwable t) {}

    @Override
    public void cancel() {}

    @Override
    public ExecutionResult getStatus() {
      return null;
    }

    @Override
    public Optional<TsBlock> getBatchResult() throws IoTDBException {
      return batches.remove();
    }

    @Override
    public Optional<ByteBuffer> getByteBufferBatchResult() {
      return Optional.empty();
    }

    @Override
    public boolean hasNextResult() {
      return !batches.isEmpty();
    }

    @Override
    public int getOutputValueColumnCount() {
      return 1;
    }

    @Override
    public DatasetHeader getDatasetHeader() {
      return datasetHeader;
    }

    @Override
    public QueryType getQueryType() {
      return QueryType.READ;
    }

    @Override
    public boolean isQuery() {
      return true;
    }

    @Override
    public boolean isUserQuery() {
      return true;
    }

    @Override
    public String getQueryId() {
      return null;
    }

    @Override
    public long getStartExecutionTime() {
      return 0;
    }

    @Override
    public void recordExecutionTime(long executionTime) {}

    @Override
    public void updateCurrentRpcStartTime(long startTime) {}

    @Override
    public boolean isActive() {
      return false;
    }

    @Override
    public long getTotalExecutionTime() {
      return 0;
    }

    @Override
    public long getTimeout() {
      return 0;
    }

    @Override
    public Optional<String> getExecuteSQL() {
      return Optional.empty();
    }

    @Override
    public String getStatementType() {
      return null;
    }

    @Override
    public SqlDialect getSQLDialect() {
      return SqlDialect.TREE;
    }

    @Override
    public String getUser() {
      return null;
    }

    @Override
    public String getClientHostname() {
      return null;
    }

    @Override
    public boolean isDebug() {
      return false;
    }
  }
}
