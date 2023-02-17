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

import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class SchemaCountOperator<T extends ISchemaInfo> implements SourceOperator {

  private static final List<TSDataType> OUTPUT_DATA_TYPES =
      Collections.singletonList(TSDataType.INT64);

  private final PlanNodeId sourceId;
  private final OperatorContext operatorContext;

  private final ISchemaSource<T> schemaSource;

  private ISchemaReader<T> schemaReader;
  private boolean isFinished;

  public SchemaCountOperator(
      PlanNodeId sourceId, OperatorContext operatorContext, ISchemaSource<T> schemaSource) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.schemaSource = schemaSource;
  }

  private final ISchemaRegion getSchemaRegion() {
    return ((SchemaDriverContext) operatorContext.getDriverContext()).getSchemaRegion();
  }

  private ISchemaReader<T> createSchemaReader() {
    return schemaSource.getSchemaReader(getSchemaRegion());
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
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(OUTPUT_DATA_TYPES);
    long count = 0;
    if (schemaReader == null) {
      schemaReader = createSchemaReader();
    }
    while (schemaReader.hasNext()) {
      schemaReader.next();
      count++;
    }
    if (!schemaReader.isSuccess()) {
      throw new RuntimeException(schemaReader.getFailure());
    }

    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeLong(count);
    tsBlockBuilder.declarePosition();
    return tsBlockBuilder.build();
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
  public long calculateMaxPeekMemory() {
    // the long used for count
    return 8L;
  }

  @Override
  public long calculateMaxReturnSize() {
    // the long used for count
    return 8L;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public void close() throws Exception {
    if (schemaReader != null) {
      schemaReader.close();
      schemaReader = null;
    }
  }
}
