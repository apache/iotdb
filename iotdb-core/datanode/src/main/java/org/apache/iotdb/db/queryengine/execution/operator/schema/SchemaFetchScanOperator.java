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
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.util.Collections;
import java.util.Iterator;
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
  private final boolean withAttributes;
  private final boolean withTemplate;
  private final boolean withAliasForce;

  private final boolean fetchDevice;
  private boolean isFinished = false;
  private final PathPatternTree authorityScope;

  private Iterator<SchemaNode> schemaNodeIteratorForSerialize = null;
  private long schemaTreeMemCost;
  private PublicBAOS baos = null;
  // Reserve some bytes to avoid capacity grow
  private static final int EXTRA_SIZE_TO_AVOID_GROW = 1024;
  private static int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SchemaFetchScanOperator.class);

  public static SchemaFetchScanOperator ofSeries(
      PlanNodeId planNodeId,
      OperatorContext context,
      PathPatternTree patternTree,
      Map<Integer, Template> templateMap,
      ISchemaRegion schemaRegion,
      boolean withTags,
      boolean withAttributes,
      boolean withTemplate,
      boolean withAliasForce) {
    return new SchemaFetchScanOperator(
        planNodeId,
        context,
        patternTree,
        templateMap,
        schemaRegion,
        withTags,
        withAttributes,
        withTemplate,
        withAliasForce);
  }

  public static SchemaFetchScanOperator ofDevice(
      PlanNodeId planNodeId,
      OperatorContext context,
      PathPatternTree patternTree,
      PathPatternTree authorityScope,
      ISchemaRegion schemaRegion) {
    return new SchemaFetchScanOperator(
        planNodeId, context, patternTree, authorityScope, schemaRegion);
  }

  private SchemaFetchScanOperator(
      PlanNodeId planNodeId,
      OperatorContext context,
      PathPatternTree patternTree,
      Map<Integer, Template> templateMap,
      ISchemaRegion schemaRegion,
      boolean withTags,
      boolean withAttributes,
      boolean withTemplate,
      boolean withAliasForce) {
    this.sourceId = planNodeId;
    this.operatorContext = context;
    this.patternTree = patternTree;
    this.schemaRegion = schemaRegion;
    this.templateMap = templateMap;
    this.withTags = withTags;
    this.withAttributes = withAttributes;
    this.withTemplate = withTemplate;
    this.withAliasForce = withAliasForce;
    this.fetchDevice = false;
    this.authorityScope = SchemaConstant.ALL_MATCH_SCOPE;
  }

  private SchemaFetchScanOperator(
      PlanNodeId planNodeId,
      OperatorContext context,
      PathPatternTree patternTree,
      PathPatternTree authorityScope,
      ISchemaRegion schemaRegion) {
    this.sourceId = planNodeId;
    this.operatorContext = context;
    this.patternTree = patternTree;
    this.schemaRegion = schemaRegion;
    this.templateMap = Collections.emptyMap();
    this.withTags = false;
    this.withAttributes = false;
    this.withTemplate = false;
    this.withAliasForce = false;
    this.fetchDevice = true;
    this.authorityScope = authorityScope;
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

    boolean isFirstBatch = schemaNodeIteratorForSerialize == null;
    prepareSchemaNodeIteratorForSerialize();
    // to indicate this binary data is a part of schema tree, and the remaining parts will be sent
    // later
    ReadWriteIOUtils.write((byte) 2, baos);
    // the estimated mem cost to deserialize the total schema tree
    if (isFirstBatch) {
      ReadWriteIOUtils.write(schemaTreeMemCost, baos);
    }
    while (schemaNodeIteratorForSerialize.hasNext()
        && baos.size() < DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES) {
      SchemaNode node = schemaNodeIteratorForSerialize.next();
      node.serializeNodeOwnContent(baos);
    }
    byte[] currentBatch = baos.toByteArray();
    baos.reset();
    isFinished = !schemaNodeIteratorForSerialize.hasNext();
    if (isFinished) {
      // indicate all continuous binary data is finished
      currentBatch[0] = 3;
      releaseSchemaTree();
      baos = null;
    }
    return new TsBlock(
        new TimeColumn(1, new long[] {0}),
        new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(currentBatch)}));
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
    releaseSchemaTree();
    baos = null;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  private void prepareSchemaNodeIteratorForSerialize() {
    if (schemaNodeIteratorForSerialize != null) {
      return;
    }
    try {
      ClusterSchemaTree schemaTree =
          fetchDevice
              ? schemaRegion.fetchDeviceSchema(patternTree, authorityScope)
              : schemaRegion.fetchSeriesSchema(
                  patternTree, templateMap, withTags, withAttributes, withTemplate, withAliasForce);
      schemaNodeIteratorForSerialize = schemaTree.getIteratorForSerialize();
      baos = new PublicBAOS(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES + EXTRA_SIZE_TO_AVOID_GROW);
      if (operatorContext != null) {
        long ramBytesUsed = schemaTree.ramBytesUsed();
        operatorContext
            .getInstanceContext()
            .getMemoryReservationContext()
            .reserveMemoryCumulatively(ramBytesUsed);
        // For temporary and independently counted memory, we need process it immediately
        operatorContext
            .getInstanceContext()
            .getMemoryReservationContext()
            .reserveMemoryImmediately();
        this.schemaTreeMemCost = ramBytesUsed;
      }
    } catch (MetadataException e) {
      throw new SchemaExecutionException(e);
    }
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

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES
        + EXTRA_SIZE_TO_AVOID_GROW
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId);
  }

  private void releaseSchemaTree() {
    if (schemaTreeMemCost <= 0 || operatorContext == null) {
      return;
    }
    operatorContext
        .getInstanceContext()
        .getMemoryReservationContext()
        .releaseMemoryCumulatively(schemaTreeMemCost);
    schemaTreeMemCost = 0;
    schemaNodeIteratorForSerialize = null;
  }

  @TestOnly
  public static void setDefaultMaxTsBlockSizeInBytes(int newSize) {
    DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES = newSize;
  }
}
