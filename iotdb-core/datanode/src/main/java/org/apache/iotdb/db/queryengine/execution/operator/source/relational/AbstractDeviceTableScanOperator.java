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

import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.DEVICE_NUMBER;

public abstract class AbstractDeviceTableScanOperator extends AbstractTableScanOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AbstractDeviceTableScanOperator.class);

  protected List<DeviceEntry> deviceEntries;
  protected int deviceCount;
  protected int currentDeviceIndex;

  protected AbstractDeviceTableScanOperator(AbstractTableScanOperatorParameter parameter) {
    super(parameter);
    this.deviceEntries = parameter.deviceEntries;
    this.deviceCount = parameter.deviceEntries.size();
    this.currentDeviceIndex = 0;
    this.operatorContext.recordSpecifiedInfo(DEVICE_NUMBER, Integer.toString(this.deviceCount));
    recordCurrentDeviceIndex();
    constructAlignedSeriesScanUtil();
  }

  @Override
  protected boolean hasCurrentDeviceEntry() {
    return currentDeviceIndex < deviceCount;
  }

  @Override
  protected DeviceEntry getCurrentDeviceEntry() {
    return deviceEntries.get(currentDeviceIndex);
  }

  @Override
  protected boolean advanceDeviceEntry() {
    currentDeviceIndex++;
    return hasCurrentDeviceEntry();
  }

  @Override
  protected void recordCurrentDeviceIndex() {
    this.operatorContext.recordSpecifiedInfo(
        CommonOperatorUtils.CURRENT_DEVICE_INDEX_STRING, Integer.toString(currentDeviceIndex));
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed()
        + INSTANCE_SIZE
        - AbstractTableScanOperator.INSTANCE_SIZE
        + RamUsageEstimator.sizeOfCollection(deviceEntries);
  }
}
