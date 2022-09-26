/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.List;
import java.util.stream.Collectors;

public class DevicesSchemaScanOperator extends SchemaQueryScanOperator {
  private final boolean hasSgCol;
  private final List<TSDataType> outputDataTypes;

  public DevicesSchemaScanOperator(
      PlanNodeId sourceId,
      OperatorContext operatorContext,
      int limit,
      int offset,
      PartialPath partialPath,
      boolean isPrefixPath,
      boolean hasSgCol) {
    super(sourceId, operatorContext, limit, offset, partialPath, isPrefixPath);
    this.hasSgCol = hasSgCol;
    this.outputDataTypes =
        (hasSgCol
                ? ColumnHeaderConstant.showDevicesWithSgColumnHeaders
                : ColumnHeaderConstant.showDevicesColumnHeaders)
            .stream().map(ColumnHeader::getColumnType).collect(Collectors.toList());
  }

  @Override
  protected TsBlock createTsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    try {
      ((SchemaDriverContext) operatorContext.getInstanceContext().getDriverContext())
          .getSchemaRegion()
          .getMatchedDevices(convertToPhysicalPlan())
          .left
          .forEach(device -> setColumns(device, builder));
    } catch (MetadataException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return builder.build();
  }

  // ToDo @xinzhongtianxia remove this temporary converter after mpp online
  private ShowDevicesPlan convertToPhysicalPlan() {
    return new ShowDevicesPlan(partialPath, limit, offset, hasSgCol);
  }

  private void setColumns(ShowDevicesResult device, TsBlockBuilder builder) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(new Binary(device.getName()));
    if (hasSgCol) {
      builder.getColumnBuilder(1).writeBinary(new Binary(device.getSgName()));
      builder.getColumnBuilder(2).writeBinary(new Binary(String.valueOf(device.isAligned())));
    } else {
      builder.getColumnBuilder(1).writeBinary(new Binary(String.valueOf(device.isAligned())));
    }
    builder.declarePosition();
  }
}
