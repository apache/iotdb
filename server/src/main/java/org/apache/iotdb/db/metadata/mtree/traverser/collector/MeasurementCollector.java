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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

// This class defines MeasurementMNode as target node and defines the measurement process framework.
public abstract class MeasurementCollector<T> extends CollectorTraverser<T> {

  public MeasurementCollector(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
    isMeasurementTraverser = true;
  }

  public MeasurementCollector(IMNode startNode, PartialPath path, int limit, int offset)
      throws MetadataException {
    super(startNode, path, limit, offset);
    isMeasurementTraverser = true;
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException {
    return false;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException {
    if (!node.isMeasurement()) {
      return false;
    }
    if (hasLimit) {
      curOffset += 1;
      if (curOffset < offset) {
        return true;
      }
    }
    collectMeasurement(node.getAsMeasurementMNode());
    if (hasLimit) {
      count += 1;
    }
    return true;
  }

  /**
   * collect the information of one measurement
   *
   * @param node MeasurementMNode holding the measurement schema
   */
  protected abstract void collectMeasurement(IMeasurementMNode node) throws MetadataException;
}
