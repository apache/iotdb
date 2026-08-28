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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryDataSourceLease;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.NonAlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.BatchDeviceEntrySource;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.InMemoryDeviceEntrySource;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DeviceIteratorScanOperator extends AbstractDataSourceOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DeviceIteratorScanOperator.class);

  private final OperatorContext operatorContext;
  private List<DeviceEntry> deviceEntries;
  private final BatchDeviceEntrySource deviceEntrySource;
  private final List<String> measurementColumnNames;
  private final List<IMeasurementSchema> measurementSchemas;
  private final Set<String> allSensors;
  private final boolean batchQueryDataSource;
  private final DeviceChildOperatorTreeGenerator deviceChildOperatorTreeGenerator;

  private QueryDataSource queryDataSource;
  private QueryDataSourceLease currentLease;
  private boolean batchLeasePending;
  private int currentDeviceIndex;
  private Operator currentDeviceRootOperator;
  private List<Operator> dataSourceOperators;
  // For each device operator tree, isBlocked needs to be called once.
  // Calling isBlocked will set this field to true.
  // When isBlocked is not called for a device, hasNext will return true and next will return null.
  private boolean currentDeviceInit;

  public DeviceIteratorScanOperator(TreeNonAlignedDeviceViewScanParameters parameter) {
    this.operatorContext = parameter.context;
    this.deviceEntries = new ArrayList<>();
    this.deviceEntrySource = parameter.deviceEntrySource;
    this.measurementColumnNames = parameter.measurementColumnNames;
    this.measurementSchemas = parameter.measurementSchemas;
    this.allSensors = parameter.allSensors;
    this.batchQueryDataSource = true;
    this.deviceChildOperatorTreeGenerator = parameter.generator;
    this.currentDeviceIndex = 0;
    this.currentDeviceInit = false;
    this.operatorContext.recordSpecifiedInfo(
        CommonOperatorUtils.CURRENT_DEVICE_INDEX_STRING, Integer.toString(0));
  }

  public DeviceIteratorScanOperator(
      OperatorContext operatorContext,
      List<DeviceEntry> deviceEntries,
      DeviceChildOperatorTreeGenerator childOperatorTreeGenerator) {
    this.operatorContext = operatorContext;
    // Keep the original non-batch lifecycle: the child tree is constructed before the
    // DataDriver (or the caller) initializes the shared QueryDataSource.
    this.deviceEntries = deviceEntries;
    this.deviceEntrySource = new InMemoryDeviceEntrySource(deviceEntries);
    this.measurementColumnNames = java.util.Collections.emptyList();
    this.measurementSchemas = java.util.Collections.emptyList();
    this.allSensors = java.util.Collections.emptySet();
    this.batchQueryDataSource = false;
    this.deviceChildOperatorTreeGenerator = childOperatorTreeGenerator;
    this.operatorContext.recordSpecifiedInfo(
        CommonOperatorUtils.CURRENT_DEVICE_INDEX_STRING, Integer.toString(0));
    constructCurrentDeviceOperatorTree();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (!batchQueryDataSource) {
      if (currentDeviceRootOperator != null && currentDeviceRootOperator.hasNext()) {
        return true;
      }
      if (!currentDeviceInit) {
        return true;
      }
      if (currentDeviceIndex + 1 >= deviceEntries.size()) {
        return false;
      }
      nextDevice();
      return true;
    }
    if (currentDeviceRootOperator == null) {
      return prepareNextDeviceBatch();
    }
    if (currentDeviceRootOperator != null && currentDeviceRootOperator.hasNext()) {
      return true;
    } else {
      if (!currentDeviceInit) {
        return true;
      }
      if (currentDeviceIndex + 1 >= deviceEntries.size()) {
        releaseCurrentBatch();
        return prepareNextDeviceBatch();
      } else {
        nextDevice();
        return true;
      }
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    if (batchLeasePending) {
      return false;
    }
    if (currentDeviceRootOperator == null) {
      return batchQueryDataSource && !deviceEntrySource.hasNextBatch();
    }
    if (currentDeviceRootOperator.hasNext() || !currentDeviceInit) {
      return false;
    }
    if (currentDeviceIndex + 1 < deviceEntries.size()) {
      return false;
    }
    return !batchQueryDataSource || !deviceEntrySource.hasNextBatch();
  }

  private boolean prepareNextDeviceBatch() throws Exception {
    while (deviceEntries.isEmpty()) {
      if (!deviceEntrySource.hasNextBatch()) {
        return false;
      }
      deviceEntries = deviceEntrySource.nextBatch();
    }

    if (batchQueryDataSource && !hasExternallyInitializedDataSource()) {
      List<IFullPath> paths = new ArrayList<>();
      for (DeviceEntry deviceEntry : deviceEntries) {
        if (deviceEntry instanceof NonAlignedDeviceEntry) {
          for (IMeasurementSchema measurementSchema : measurementSchemas) {
            paths.add(new NonAlignedFullPath(deviceEntry.getDeviceID(), measurementSchema));
          }
        } else {
          paths.add(
              new AlignedFullPath(
                  deviceEntry.getDeviceID(),
                  measurementColumnNames,
                  measurementSchemas,
                  allSensors));
        }
      }
      currentLease = operatorContext.getInstanceContext().initBatchQueryDataSource(paths);
      if (currentLease == null) {
        batchLeasePending = true;
        return false;
      }
      batchLeasePending = false;
      queryDataSource = currentLease.getDataSource();
    }
    currentDeviceIndex = 0;
    constructCurrentDeviceOperatorTree();
    initQueryDataSource(queryDataSource);
    return true;
  }

  private boolean hasExternallyInitializedDataSource() {
    return queryDataSource != null
        && (queryDataSource.getSeqResourcesSize() > 0
            || queryDataSource.getUnseqResourcesSize() > 0);
  }

  private void releaseCurrentBatch() throws Exception {
    if (!batchQueryDataSource) {
      if (currentDeviceRootOperator != null) {
        currentDeviceRootOperator.close();
      }
      currentDeviceRootOperator = null;
      dataSourceOperators = null;
      return;
    }
    if (currentDeviceRootOperator != null) {
      deviceChildOperatorTreeGenerator.getCurrentDeviceStartCloseOperator().close();
    }
    currentDeviceRootOperator = null;
    dataSourceOperators = null;
    deviceEntries = new ArrayList<>();
    currentDeviceIndex = 0;
    currentDeviceInit = false;
    queryDataSource = null;
    if (currentLease != null) {
      currentLease.close();
      currentLease = null;
    }
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
        CommonOperatorUtils.CURRENT_DEVICE_INDEX_STRING, Integer.toString(currentDeviceIndex));
  }

  private void constructCurrentDeviceOperatorTree() {
    if (this.deviceEntries.isEmpty()) {
      return;
    }
    if (this.deviceEntries.get(this.currentDeviceIndex) == null) {
      throw new IllegalStateException(
          String.format(
              DataNodeQueryMessages.QUERY_EXCEPTION_DEVICE_ENTRIES_OF_INDEX_S_IS_EMPTY_BCFB0644,
              this.currentDeviceIndex));
    }
    DeviceEntry deviceEntry = this.deviceEntries.get(this.currentDeviceIndex);

    deviceChildOperatorTreeGenerator.generateCurrentDeviceOperatorTree(deviceEntry, true);
    currentDeviceRootOperator = deviceChildOperatorTreeGenerator.getCurrentDeviceRootOperator();
    dataSourceOperators = deviceChildOperatorTreeGenerator.getCurrentDeviceDataSourceOperators();
    currentDeviceInit = false;
  }

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    this.queryDataSource = (QueryDataSource) dataSource;
    if (dataSourceOperators == null || dataSourceOperators.isEmpty()) {
      return;
    }
    for (Operator operator : dataSourceOperators) {
      ((AbstractDataSourceOperator) operator).initQueryDataSource(dataSource);
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasNext()) {
      return null;
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
    if (currentDeviceRootOperator == null) {
      return NOT_BLOCKED;
    }
    currentDeviceInit = true;
    return currentDeviceRootOperator.isBlocked();
  }

  @Override
  public void close() throws Exception {
    releaseCurrentBatch();
    deviceEntrySource.close();
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    throw new UnsupportedOperationException(
        DataNodeQueryMessages
            .QUERY_EXCEPTION_SHOULD_NOT_CALL_GETRESULTDATATYPES_METHOD_IN_DEVICEITERATORSCANOPERATOR_E915A153);
  }

  @Override
  public long calculateMaxPeekMemory() {
    return deviceChildOperatorTreeGenerator.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return deviceChildOperatorTreeGenerator.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return deviceChildOperatorTreeGenerator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(currentDeviceRootOperator)
        + RamUsageEstimator.sizeOfCollection(deviceEntries)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(
            deviceChildOperatorTreeGenerator);
  }

  public DeviceChildOperatorTreeGenerator getDeviceChildOperatorTreeGenerator() {
    return deviceChildOperatorTreeGenerator;
  }

  public static class TreeNonAlignedDeviceViewScanParameters {
    public final OperatorContext context;
    public final List<String> measurementColumnNames;
    public final Set<String> allSensors;
    public final List<IMeasurementSchema> measurementSchemas;
    public final BatchDeviceEntrySource deviceEntrySource;
    public final DeviceChildOperatorTreeGenerator generator;

    public TreeNonAlignedDeviceViewScanParameters(
        Set<String> allSensors,
        OperatorContext context,
        List<String> measurementColumnNames,
        List<IMeasurementSchema> measurementSchemas,
        BatchDeviceEntrySource deviceEntrySource,
        DeviceChildOperatorTreeGenerator generator) {
      this.allSensors = allSensors;
      this.context = context;
      this.measurementColumnNames = measurementColumnNames;
      this.measurementSchemas = measurementSchemas;
      this.deviceEntrySource = deviceEntrySource;
      this.generator = generator;
    }
  }

  public interface DeviceChildOperatorTreeGenerator extends Accountable {
    // Do the offset and limit operator need to keep after the device iterator
    boolean keepOffsetAndLimitOperatorAfterDeviceIterator();

    // Generate the following operator subtree based on the current deviceEntry
    void generateCurrentDeviceOperatorTree(DeviceEntry deviceEntry, boolean needAdaptor);

    // Returns the root operator of the subtree
    Operator getCurrentDeviceRootOperator();

    // Returns all DataSourceOperators created this time for use in initQueryDataSource in
    // DeviceIterator
    List<Operator> getCurrentDeviceDataSourceOperators();

    // Returns which operator to close after switching device
    Operator getCurrentDeviceStartCloseOperator();

    // Estimates one device's child operator tree without fetching a DeviceEntry batch.
    default long calculateMaxPeekMemory() {
      return 0;
    }

    default long calculateMaxReturnSize() {
      return 0;
    }

    default long calculateRetainedSizeAfterCallingNext() {
      return 0;
    }
  }
}
