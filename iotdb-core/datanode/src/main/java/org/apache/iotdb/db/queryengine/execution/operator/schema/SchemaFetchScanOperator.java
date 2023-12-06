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

package org.apache.iotdb.db.queryengine.execution.operator.schema;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

public class SchemaFetchScanOperator implements SourceOperator {
  private final PlanNodeId sourceId;
  private final OperatorContext operatorContext;
  private final PathPatternTree patternTree;
  private final Map<Integer, Template> templateMap;

  private final ISchemaRegion schemaRegion;
  private final boolean withTags;
  private final boolean withTemplate;
  private boolean isFinished = false;

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  public SchemaFetchScanOperator(
      PlanNodeId planNodeId,
      OperatorContext context,
      PathPatternTree patternTree,
      Map<Integer, Template> templateMap,
      ISchemaRegion schemaRegion,
      boolean withTags,
      boolean withTemplate) {
    this.sourceId = planNodeId;
    this.operatorContext = context;
    this.patternTree = patternTree;
    this.schemaRegion = schemaRegion;
    this.templateMap = templateMap;
    this.withTags = withTags;
    this.withTemplate = withTemplate;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    isFinished = true;
    try {
      return fetchSchema();
    } catch (MetadataException e) {
      throw new SchemaExecutionException(e);
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    return !isFinished;
  }

  @Override
  public boolean isFinished() throws Exception {
    return isFinished;
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  private TsBlock fetchSchema() throws MetadataException {
    ClusterSchemaTree schemaTree =
        schemaRegion.fetchSchema(patternTree, templateMap, withTags, withTemplate);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      // to indicate this binary data is database info
      ReadWriteIOUtils.write((byte) 1, outputStream);

      schemaTree.serialize(outputStream);
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
    }
    return new TsBlock(
        new TimeColumn(1, new long[] {0}),
        new BinaryColumn(
            1, Optional.empty(), new Binary[] {new Binary(outputStream.toByteArray())}));
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
    return 0L;
  }
}
