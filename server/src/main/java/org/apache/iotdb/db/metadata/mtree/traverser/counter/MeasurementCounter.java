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
package org.apache.iotdb.db.metadata.mtree.traverser.counter;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;

// This method implements the measurement count function.
// One MultiMeasurement will only be count once.
public class MeasurementCounter extends CounterTraverser {

  private boolean hasTag;

  public MeasurementCounter(IMNode startNode, PartialPath path, IMTreeStore store, boolean hasTag)
      throws MetadataException {
    super(startNode, path, store);
    isMeasurementTraverser = true;
    this.hasTag = hasTag;
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
    return false;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
    if (!node.isMeasurement()) {
      return false;
    }
    if (hasTag) {
      if (node.getAsMeasurementMNode().getOffset() == -1) {
        return true;
      }
    }
    count++;
    return true;
  }
}
