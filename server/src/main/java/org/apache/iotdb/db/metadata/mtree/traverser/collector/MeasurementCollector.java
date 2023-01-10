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
package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.basic.MeasurementTraverser;

// This class defines MeasurementMNode as target node and defines the measurement process framework.
// TODO: set R is ITimeseriesInfo
public abstract class MeasurementCollector<R> extends MeasurementTraverser<R> {

  protected MeasurementCollector(
      IMNode startNode, PartialPath path, IMTreeStore store, boolean isPrefixMatch)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch);
  }

  @Override
  protected R generateResult(IMNode nextMatchedNode) {
    return collectMeasurement(nextMatchedNode.getAsMeasurementMNode());
  }

  /**
   * collect the information of one measurement
   *
   * @param node MeasurementMNode holding the measurement schema
   */
  protected abstract R collectMeasurement(IMeasurementMNode node);

  /**
   * When traverse goes into a template, IMNode.getPartialPath may not work as nodes in template has
   * no parent on MTree. So this methods will construct a path from root to node in template using a
   * stack traverseContext.
   */
  protected MeasurementPath getCurrentMeasurementPathInTraverse(IMeasurementMNode currentNode) {
    IMNode par = getParentOfNextMatchedNode();
    MeasurementPath retPath =
        new MeasurementPath(getPartialPathFromRootToNode(currentNode), currentNode.getSchema());
    retPath.setUnderAlignedEntity(par.getAsEntityMNode().isAligned());
    return retPath;
  }
}
