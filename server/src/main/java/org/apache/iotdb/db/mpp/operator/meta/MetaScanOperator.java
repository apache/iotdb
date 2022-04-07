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
package org.apache.iotdb.db.mpp.operator.meta;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.schemaregion.SchemaRegion;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.operator.source.SourceOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

public abstract class MetaScanOperator implements SourceOperator {

  protected OperatorContext operatorContext;
  protected TsBlock tsBlock;
  private boolean hasCachedTsBlock;

  protected SchemaRegion schemaRegion;
  protected int limit;
  protected int offset;
  protected PartialPath partialPath;
  protected boolean isPrefixPath;
  protected List<String> columns;

  protected MetaScanOperator(
      OperatorContext operatorContext,
      ConsensusGroupId schemaRegionId,
      int limit,
      int offset,
      PartialPath partialPath,
      boolean isPrefixPath,
      List<String> columns) {
    this.operatorContext = operatorContext;
    this.schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);
    this.limit = limit;
    this.offset = offset;
    this.partialPath = partialPath;
    this.isPrefixPath = isPrefixPath;
    this.columns = columns;
  }

  protected abstract TsBlock createTsBlock() throws MetadataException;

  public PartialPath getPartialPath() {
    return partialPath;
  }

  public SchemaRegion getSchemaRegion() {
    return schemaRegion;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public boolean isPrefixPath() {
    return isPrefixPath;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws IOException {
    hasCachedTsBlock = false;
    return tsBlock;
  }

  @Override
  public boolean hasNext() throws IOException {
    try {
      if (tsBlock == null) {
        tsBlock = createTsBlock();
        if (tsBlock.getPositionCount() > 0) {
          hasCachedTsBlock = true;
        }
      }
      return hasCachedTsBlock;
    } catch (MetadataException e) {
      throw new IOException(e);
    }
  }
}
