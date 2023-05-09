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

package org.apache.iotdb.db.mpp.plan.execution;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.LastQueryStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class FastLastQueryExecution implements IQueryExecution {

  private final LastQueryStatement rawStatement;
  private final MPPQueryContext context;

  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;

  private final TsBlockBuilder resultTsBlockBuilder;

  private long totalExecutionTime;

  private final DatasetHeader lastDatasetHeader = DatasetHeaderFactory.getLastQueryHeader();

  private final DataNodeSchemaCache DATA_NODE_SCHEMA_CACHE = DataNodeSchemaCache.getInstance();

  public FastLastQueryExecution(
      Statement rawStatement,
      MPPQueryContext context,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    this.rawStatement = (LastQueryStatement) rawStatement;
    this.context = context;
    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;

    this.resultTsBlockBuilder = new TsBlockBuilder(lastDatasetHeader.getRespDataTypes());
  }

  @Override
  public void start() {
    List<PartialPath> pathPatterns = rawStatement.getFromComponent().getPrefixPaths();

    PathPatternTree pathPatternTree = new PathPatternTree();
    pathPatterns.forEach(pathPatternTree::appendPathPattern);
    ISchemaTree schemaTree = schemaFetcher.fetchSchema(pathPatternTree, context);

    for (PartialPath pathPattern : pathPatterns) {
      List<MeasurementPath> measurementPaths = schemaTree.searchMeasurementPaths(pathPattern).left;
      for (MeasurementPath measurementPath : measurementPaths) {
        TimeValuePair timeValuePair = DATA_NODE_SCHEMA_CACHE.getLastCache(measurementPath);
        if (timeValuePair != null) {
          appendLastValue(
              timeValuePair.getTimestamp(),
              measurementPath.toString(),
              timeValuePair.getValue().getStringValue(),
              measurementPath.getSeriesType());
        }
      }
    }
  }

  private void appendLastValue(
      long lastTime, String fullPath, String lastValue, TSDataType dataType) {
    // Time
    resultTsBlockBuilder.getTimeColumnBuilder().writeLong(lastTime);
    // timeseries
    resultTsBlockBuilder.getColumnBuilder(0).writeBinary(Binary.valueOf(fullPath));
    // value
    resultTsBlockBuilder.getColumnBuilder(1).writeBinary(Binary.valueOf(lastValue));
    // dataType
    resultTsBlockBuilder.getColumnBuilder(2).writeBinary(Binary.valueOf(dataType.toString()));
    resultTsBlockBuilder.declarePosition();
  }

  @Override
  public void stop(Throwable t) {
    // do nothing
  }

  @Override
  public void stopAndCleanup() {
    // do nothing
  }

  @Override
  public void stopAndCleanup(Throwable t) {
    // do nothing
  }

  @Override
  public void cancel() {
    // do nothing
  }

  @Override
  public ExecutionResult getStatus() {
    return new ExecutionResult(context.getQueryId(), RpcUtils.SUCCESS_STATUS);
  }

  @Override
  public Optional<TsBlock> getBatchResult() throws IoTDBException {
    TsBlock result = resultTsBlockBuilder.build();
    resultTsBlockBuilder.reset();
    return Optional.ofNullable(result);
  }

  @Override
  public Optional<ByteBuffer> getByteBufferBatchResult() throws IoTDBException {
    try {
      TsBlock result = resultTsBlockBuilder.build();
      resultTsBlockBuilder.reset();
      if (result.isEmpty()) {
        return Optional.empty();
      }
      return Optional.ofNullable(new TsBlockSerde().serialize(result));
    } catch (IOException e) {
      throw new IoTDBException(e, TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    }
  }

  @Override
  public boolean hasNextResult() {
    return false;
  }

  @Override
  public int getOutputValueColumnCount() {
    return lastDatasetHeader.getOutputValueColumnCount();
  }

  @Override
  public DatasetHeader getDatasetHeader() {
    return lastDatasetHeader;
  }

  @Override
  public boolean isQuery() {
    return true;
  }

  @Override
  public String getQueryId() {
    return context.getQueryId().getId();
  }

  @Override
  public long getStartExecutionTime() {
    return context.getStartTime();
  }

  @Override
  public void recordExecutionTime(long executionTime) {
    totalExecutionTime += executionTime;
  }

  @Override
  public long getTotalExecutionTime() {
    return totalExecutionTime;
  }

  @Override
  public Optional<String> getExecuteSQL() {
    return Optional.ofNullable(context.getSql());
  }

  @Override
  public Statement getStatement() {
    return rawStatement;
  }
}
