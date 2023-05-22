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
package org.apache.iotdb.db.metadata.mtree.traverser.basic;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.DataTypeFilter;
import org.apache.iotdb.commons.schema.filter.impl.PathContainsFilter;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.Traverser;

import org.apache.commons.lang.StringUtils;

public abstract class MeasurementTraverser<R, N extends IMNode<N>> extends Traverser<R, N> {

  private final MeasurementFilterVisitor filterVisitor = new MeasurementFilterVisitor();

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @param store MTree store to traverse
   * @param isPrefixMatch prefix match or not
   * @throws MetadataException path does not meet the expected rules
   */
  public MeasurementTraverser(
      N startNode, PartialPath path, IMTreeStore<N> store, boolean isPrefixMatch)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch);
  }

  @Override
  protected boolean mayTargetNodeType(N node) {
    return node.isMeasurement() && filterVisitor.process(schemaFilter, node);
  }

  @Override
  protected boolean acceptFullMatchedNode(N node) {
    return node.isMeasurement() && filterVisitor.process(schemaFilter, node);
  }

  @Override
  protected boolean acceptInternalMatchedNode(N node) {
    return false;
  }

  @Override
  protected boolean shouldVisitSubtreeOfFullMatchedNode(N node) {
    return !node.isMeasurement();
  }

  @Override
  protected boolean shouldVisitSubtreeOfInternalMatchedNode(N node) {
    return !node.isMeasurement();
  }

  class MeasurementFilterVisitor extends SchemaFilterVisitor<Boolean, N> {
    @Override
    public Boolean visitNode(SchemaFilter filter, N node) {
      return true;
    }

    @Override
    public Boolean visitPathContainsFilter(PathContainsFilter pathContainsFilter, N node) {
      if (pathContainsFilter.getContainString() == null) {
        return true;
      }
      return StringUtils.join(
              getFullPathFromRootToNode(node.getAsMNode()), IoTDBConstant.PATH_SEPARATOR)
          .toLowerCase()
          .contains(pathContainsFilter.getContainString());
    }

    @Override
    public Boolean visitDataTypeFilter(DataTypeFilter dataTypeFilter, N node) {
      return node.getAsMeasurementMNode().getSchema().getType() == dataTypeFilter.getDataType();
    }
  }
}
