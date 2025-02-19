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
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractSeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class DeviceIteratorScanOperator extends AbstractDataSourceOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DeviceIteratorScanOperator.class);

  private final OperatorContext operatorContext;
  private final List<DeviceEntry> deviceEntries;
  private final DeviceChildOperatorTreeGenerator deviceChildOperatorTreeGenerator;

  private QueryDataSource queryDataSource;
  private int currentDeviceIndex;
  private Operator currentDeviceRootOperator;
  private List<Operator> seriesScanOperators;
  private boolean currentDeviceInit;

  public DeviceIteratorScanOperator(
      OperatorContext operatorContext,
      List<DeviceEntry> deviceEntries,
      DeviceChildOperatorTreeGenerator childOperatorTreeGenerator) {
    this.operatorContext = operatorContext;
    this.deviceEntries = deviceEntries;
    this.deviceChildOperatorTreeGenerator = childOperatorTreeGenerator;
    this.currentDeviceIndex = 0;
    this.currentDeviceInit = false;
    this.operatorContext.recordSpecifiedInfo(
        AbstractTableScanOperator.CURRENT_DEVICE_INDEX_STRING, Integer.toString(0));
    constructCurrentDeviceOperatorTree();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (currentDeviceRootOperator != null && currentDeviceRootOperator.hasNext()) {
      return true;
    } else {
      if (currentDeviceIndex + 1 >= deviceEntries.size()) {
        return false;
      } else {
        nextDevice();
        return true;
      }
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNext();
  }

  private void nextDevice() throws Exception {
    currentDeviceIndex++;
    deviceChildOperatorTreeGenerator.getCurrentDeviceStartCloseOperator().close();
    if (currentDeviceIndex >= deviceEntries.size()) {
      return;
    }
    constructCurrentDeviceOperatorTree();
    queryDataSource.reset();
    initQueryDataSource(queryDataSource);
    this.operatorContext.recordSpecifiedInfo(
        AbstractTableScanOperator.CURRENT_DEVICE_INDEX_STRING,
        Integer.toString(currentDeviceIndex));
  }

  private void constructCurrentDeviceOperatorTree() {
    if (this.deviceEntries.isEmpty()) {
      return;
    }
    if (this.deviceEntries.get(this.currentDeviceIndex) == null) {
      throw new IllegalStateException(
          "Device entries of index " + this.currentDeviceIndex + " is empty");
    }
    DeviceEntry deviceEntry = this.deviceEntries.get(this.currentDeviceIndex);

    deviceChildOperatorTreeGenerator.generateCurrentDeviceRootOperator(deviceEntry);
    currentDeviceRootOperator = deviceChildOperatorTreeGenerator.getCurrentDeviceRootOperator();
    seriesScanOperators = deviceChildOperatorTreeGenerator.getCurrentDeviceDataSourceOperators();
    currentDeviceInit = false;
  }

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    this.queryDataSource = (QueryDataSource) dataSource;
    if (seriesScanOperators == null || seriesScanOperators.isEmpty()) {
      return;
    }
    for (Operator operator : seriesScanOperators) {
      ((AbstractSeriesScanOperator) operator).initQueryDataSource(dataSource);
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (!currentDeviceInit) {
      return null;
    }
    return currentDeviceRootOperator.next();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    currentDeviceInit = true;
    return currentDeviceRootOperator.isBlocked();
  }

  @Override
  public void close() throws Exception {
    if (currentDeviceRootOperator != null) {
      currentDeviceRootOperator.close();
    }
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    throw new UnsupportedOperationException(
        "Should not call getResultDataTypes() method in DeviceIteratorScanOperator");
  }

  @Override
  public long calculateMaxPeekMemory() {
    return currentDeviceRootOperator.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return currentDeviceRootOperator.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return currentDeviceRootOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(currentDeviceRootOperator)
        + RamUsageEstimator.sizeOfCollection(deviceEntries);
  }

  public static class TreeNonAlignedDeviceViewScanParameters {
    public final OperatorContext context;
    public final List<DeviceEntry> deviceEntries;
    public final List<String> measurementColumnNames;
    public final Set<String> allSensors;
    public final List<IMeasurementSchema> measurementSchemas;
    public final DeviceChildOperatorTreeGenerator generator;

    public TreeNonAlignedDeviceViewScanParameters(
        Set<String> allSensors,
        OperatorContext context,
        List<DeviceEntry> deviceEntries,
        List<String> measurementColumnNames,
        List<IMeasurementSchema> measurementSchemas,
        DeviceChildOperatorTreeGenerator generator) {
      this.allSensors = allSensors;
      this.context = context;
      this.deviceEntries = deviceEntries;
      this.measurementColumnNames = measurementColumnNames;
      this.measurementSchemas = measurementSchemas;
      this.generator = generator;
    }

    public boolean isSingleColumn() {
      return measurementSchemas.size() == 1;
    }
  }

  public interface DeviceChildOperatorTreeGenerator {
    boolean keepRootOffsetAndLimitOperator();

    void generateCurrentDeviceRootOperator(DeviceEntry deviceEntry);

    Operator getCurrentDeviceRootOperator();

    List<Operator> getCurrentDeviceDataSourceOperators();

    Operator getCurrentDeviceStartCloseOperator();
  }
}
