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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.pipe.receiver.runtime.PipeReceiverRuntimeRegistry;
import org.apache.iotdb.commons.pipe.receiver.runtime.PipeReceiverRuntimeSnapshot;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ShowReceiversOperator implements SourceOperator {

  private final OperatorContext operatorContext;
  private final PlanNodeId sourceId;

  private TsBlock tsBlock;
  private boolean hasConsumed;

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ShowReceiversOperator.class);

  public ShowReceiversOperator(OperatorContext operatorContext, PlanNodeId sourceId) {
    this.operatorContext = operatorContext;
    this.sourceId = sourceId;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    TsBlock res = tsBlock;
    hasConsumed = true;
    tsBlock = null;
    return res;
  }

  @Override
  public boolean hasNext() {
    if (hasConsumed) {
      return false;
    }
    if (tsBlock == null) {
      tsBlock = buildTsBlock();
    }
    return true;
  }

  @Override
  public boolean isFinished() {
    return hasConsumed;
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public long calculateMaxPeekMemory() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateMaxReturnSize() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  private TsBlock buildTsBlock() {
    final List<TSDataType> outputDataTypes =
        DatasetHeaderFactory.getShowReceiversHeader().getRespDataTypes();
    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    final TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    final ColumnBuilder[] columnBuilders = builder.getValueColumnBuilders();
    final long currentTime =
        TimestampPrecisionUtils.convertToCurrPrecision(
            System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    for (PipeReceiverRuntimeSnapshot snapshot :
        PipeReceiverRuntimeRegistry.getInstance().snapshot()) {
      timeColumnBuilder.writeLong(currentTime);
      columnBuilders[0].writeBinary(BytesUtils.valueOf(snapshot.getReceiverNodeType()));
      writeReceiverNodeId(columnBuilders[1], snapshot);
      columnBuilders[2].writeBinary(BytesUtils.valueOf(snapshot.getProtocol()));
      columnBuilders[3].writeBinary(BytesUtils.valueOf(snapshot.getSenderAddress()));
      columnBuilders[4].writeBinary(BytesUtils.valueOf(snapshot.getSenderPorts()));
      columnBuilders[5].writeInt(snapshot.getConnectionCount());
      columnBuilders[6].writeInt(snapshot.getPipeCount());
      columnBuilders[7].writeBinary(BytesUtils.valueOf(snapshot.getPipeIds()));
      columnBuilders[8].writeBinary(BytesUtils.valueOf(snapshot.getUserName()));
      columnBuilders[9].writeBinary(BytesUtils.valueOf(snapshot.getSenderClusterId()));
      columnBuilders[10].writeBinary(
          BytesUtils.valueOf(formatTime(snapshot.getLastHandshakeTime())));
      columnBuilders[11].writeBinary(
          BytesUtils.valueOf(formatTime(snapshot.getLastTransferTime())));
      builder.declarePosition();
    }
    return builder.build();
  }

  private static void writeReceiverNodeId(
      ColumnBuilder columnBuilder, PipeReceiverRuntimeSnapshot snapshot) {
    if (snapshot.isReceiverNodeIdKnown()) {
      columnBuilder.writeInt(snapshot.getReceiverNodeId());
    } else {
      columnBuilder.appendNull();
    }
  }

  private static String formatTime(long timestampInMillis) {
    return timestampInMillis <= 0
        ? PipeReceiverRuntimeRegistry.UNKNOWN
        : DateTimeUtils.convertLongToDate(timestampInMillis, "ms");
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId);
  }
}
