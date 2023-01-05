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
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.query.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;

import java.util.stream.Collectors;

public class DevicesCountOperator extends SchemaCountOperator<IDeviceSchemaInfo> {

  private final PartialPath partialPath;
  private final boolean isPrefixPath;

  public DevicesCountOperator(
      PlanNodeId sourceId,
      OperatorContext operatorContext,
      PartialPath partialPath,
      boolean isPrefixPath) {
    super(
        sourceId,
        operatorContext,
        ColumnHeaderConstant.countDevicesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList()));

    this.partialPath = partialPath;
    this.isPrefixPath = isPrefixPath;
  }

  @Override
  protected ISchemaReader<IDeviceSchemaInfo> createSchemaReader() {
    try {
      return getSchemaRegion()
          .getDeviceReader(
              SchemaRegionReadPlanFactory.getShowDevicesPlan(partialPath, isPrefixPath));
    } catch (MetadataException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}
