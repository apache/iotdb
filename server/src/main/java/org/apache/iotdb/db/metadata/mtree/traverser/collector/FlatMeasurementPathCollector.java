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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MultiMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.UnaryMeasurementMNode;

import java.util.LinkedList;
import java.util.List;

// This class implements the measurement path collection function.
public class FlatMeasurementPathCollector extends FlatMeasurementCollector<List<PartialPath>> {

  public FlatMeasurementPathCollector(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
  }

  public FlatMeasurementPathCollector(IMNode startNode, PartialPath path, int limit, int offset)
      throws MetadataException {
    super(startNode, path, limit, offset);
    this.resultSet = new LinkedList<>();
  }

  @Override
  protected void collectUnaryMeasurement(UnaryMeasurementMNode node) throws MetadataException {
    PartialPath path = node.getPartialPath();
    if (nodes[nodes.length - 1].equals(node.getAlias())) {
      // only when user query with alias, the alias in path will be set
      path.setMeasurementAlias(node.getAlias());
    }
    resultSet.add(path);
  }

  @Override
  protected void collectMultiMeasurementComponent(MultiMeasurementMNode node, int index)
      throws MetadataException {
    resultSet.add(
        new VectorPartialPath(node.getFullPath(), node.getSubMeasurementList().get(index)));
  }
}
