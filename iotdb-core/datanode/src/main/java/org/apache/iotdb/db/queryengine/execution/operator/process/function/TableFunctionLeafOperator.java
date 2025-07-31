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

package org.apache.iotdb.db.queryengine.execution.operator.process.function;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

// only one input source is supported now
public class TableFunctionLeafOperator implements ProcessOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableFunctionLeafOperator.class);

  private final OperatorContext operatorContext;
  private final TsBlockBuilder blockBuilder;

  private final TableFunctionLeafProcessor processor;
  private volatile boolean init = false;

  public TableFunctionLeafOperator(
      OperatorContext operatorContext,
      TableFunctionProcessorProvider processorProvider,
      List<TSDataType> outputDataTypes) {
    this.operatorContext = operatorContext;
    this.processor = processorProvider.getSplitProcessor();
    this.blockBuilder = new TsBlockBuilder(outputDataTypes);
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (!init) {
      init = true;
      processor.beforeStart();
    }
    return NOT_BLOCKED;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return this.operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    List<ColumnBuilder> columnBuilders = getOutputColumnBuilders();
    try {
      processor.process(columnBuilders);
    } catch (Exception e) {
      LOGGER.warn("Exception happened when executing UDTF: ", e);
      throw new IoTDBRuntimeException(
          e.getMessage(), TSStatusCode.EXECUTE_UDF_ERROR.getStatusCode(), true);
    }
    return buildTsBlock(columnBuilders);
  }

  private List<ColumnBuilder> getOutputColumnBuilders() {
    blockBuilder.reset();
    return Arrays.asList(blockBuilder.getValueColumnBuilders());
  }

  private TsBlock buildTsBlock(List<ColumnBuilder> columnBuilders) {
    int positionCount = columnBuilders.get(0).getPositionCount();
    blockBuilder.declarePositions(positionCount);
    return blockBuilder.build(new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, positionCount));
  }

  @Override
  public boolean hasNext() throws Exception {
    return !processor.isFinish();
  }

  @Override
  public void close() throws Exception {
    try {
      processor.beforeDestroy();
    } catch (Exception e) {
      LOGGER.warn("Exception happened when executing UDTF: ", e);
      throw new IoTDBRuntimeException(
          e.getMessage(), TSStatusCode.EXECUTE_UDF_ERROR.getStatusCode(), true);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return processor.isFinish();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
