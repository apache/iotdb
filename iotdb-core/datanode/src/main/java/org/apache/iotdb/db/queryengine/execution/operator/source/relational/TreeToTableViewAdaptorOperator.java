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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

public class TreeToTableViewAdaptorOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TreeToTableViewAdaptorOperator.class);
  private final OperatorContext operatorContext;
  private final DeviceEntry currentDeviceEntry;
  private final int[] columnsIndexArray;
  private final List<ColumnSchema> columnSchemas;
  private final Operator child;
  private final IDeviceID.TreeDeviceIdColumnValueExtractor extractor;

  public TreeToTableViewAdaptorOperator(
      OperatorContext operatorContext,
      DeviceEntry deviceEntry,
      int[] columnIndexArray,
      List<ColumnSchema> columnSchemas,
      Operator child,
      IDeviceID.TreeDeviceIdColumnValueExtractor extractor) {
    this.operatorContext = operatorContext;
    this.currentDeviceEntry = deviceEntry;
    this.columnsIndexArray = columnIndexArray;
    this.columnSchemas = columnSchemas;
    this.child = child;
    this.extractor = extractor;
  }

  @Override
  public boolean hasNext() throws Exception {
    return child.hasNext();
  }

  @Override
  public TsBlock next() throws Exception {
    TsBlock measurementDataBlock = child.next();
    return MeasurementToTableViewAdaptorUtils.toTableBlock(
        measurementDataBlock,
        columnsIndexArray,
        columnSchemas,
        currentDeviceEntry,
        idColumnIndex -> getNthIdColumnValue(currentDeviceEntry, idColumnIndex));
  }

  private String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex) {
    return (String) extractor.extract(deviceEntry.getDeviceID(), idColumnIndex);
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public boolean isFinished() throws Exception {
    return child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(currentDeviceEntry)
        + RamUsageEstimator.sizeOf(columnsIndexArray)
        + RamUsageEstimator.sizeOfCollection(columnSchemas);
  }
}
