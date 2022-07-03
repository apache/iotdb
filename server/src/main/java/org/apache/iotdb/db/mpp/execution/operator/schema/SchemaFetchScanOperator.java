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

package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class SchemaFetchScanOperator implements SourceOperator {

  private static final Logger logger = LoggerFactory.getLogger(SchemaFetchScanOperator.class);

  private final PlanNodeId sourceId;
  private final OperatorContext operatorContext;
  private final PathPatternTree patternTree;
  private final ISchemaRegion schemaRegion;

  private TsBlock tsBlock;
  private boolean isFinished = false;

  public SchemaFetchScanOperator(
      PlanNodeId planNodeId,
      OperatorContext context,
      PathPatternTree patternTree,
      ISchemaRegion schemaRegion) {
    this.sourceId = planNodeId;
    this.operatorContext = context;
    this.patternTree = patternTree;
    this.schemaRegion = schemaRegion;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    isFinished = true;
    try {
      fetchSchema();
    } catch (MetadataException e) {
      logger.error("Error occurred during execute SchemaFetchOperator {}", sourceId, e);
      throw new RuntimeException(e);
    }
    return tsBlock;
  }

  @Override
  public boolean hasNext() {
    return !isFinished;
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  private void fetchSchema() throws MetadataException {
    SchemaTree schemaTree = new SchemaTree();
    List<PartialPath> partialPathList = patternTree.getAllPathPatterns();
    for (PartialPath path : partialPathList) {
      schemaTree.appendMeasurementPaths(schemaRegion.getMeasurementPaths(path, false));
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      schemaTree.serialize(outputStream);
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
    }
    this.tsBlock =
        new TsBlock(
            new TimeColumn(1, new long[] {0}),
            new BinaryColumn(
                1,
                Optional.of(new boolean[] {false}),
                new Binary[] {new Binary(outputStream.toByteArray())}));
  }
}
