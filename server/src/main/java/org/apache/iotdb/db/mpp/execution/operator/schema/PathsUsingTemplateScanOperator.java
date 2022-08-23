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
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.List;
import java.util.stream.Collectors;

public class PathsUsingTemplateScanOperator extends SchemaQueryScanOperator {

  private final int templateId;

  private final List<TSDataType> outputDataTypes;

  public PathsUsingTemplateScanOperator(
      PlanNodeId planNodeId, OperatorContext context, int templateId) {
    super(planNodeId, context, 0, 0, null, false);
    this.templateId = templateId;
    this.outputDataTypes =
        ColumnHeaderConstant.showPathsUsingTemplateHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
  }

  @Override
  protected TsBlock createTsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    try {
      ((SchemaDriverContext) operatorContext.getInstanceContext().getDriverContext())
          .getSchemaRegion()
          .getPathsUsingTemplate(templateId)
          .forEach(path -> setColumns(path, builder));
    } catch (MetadataException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return builder.build();
  }

  private void setColumns(String path, TsBlockBuilder builder) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(new Binary(path));
    builder.declarePosition();
  }
}
