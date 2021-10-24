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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;

import java.util.List;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

// This method implements the measurement component count function.
// Each component of MultiMeasurement will be count once.
// E.g. given MTree: root.sg.d.s, root.sg.d.vector[s1, s2]; FlatMeasurementCount will be 3.
public class FlatMeasurementCounter extends CounterTraverser {
  public FlatMeasurementCounter(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
    isMeasurementTraverser = true;
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException {
    if (!node.isMeasurement() || idx != nodes.length - 2) {
      return false;
    }
    IMeasurementMNode measurementMNode = node.getAsMeasurementMNode();
    if (measurementMNode.isMultiMeasurement()) {
      List<String> measurements =
          measurementMNode.getAsMultiMeasurementMNode().getSubMeasurementList();
      String regex = nodes[idx + 1].replace("*", ".*");
      for (String measurement : measurements) {
        if (Pattern.matches(regex, measurement)) {
          count++;
        }
      }
    }
    return true;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException {
    if (!node.isMeasurement()) {
      return false;
    }
    IMeasurementMNode measurementMNode = node.getAsMeasurementMNode();
    if (measurementMNode.isUnaryMeasurement()) {
      count++;
    } else if (measurementMNode.isMultiMeasurement()) {
      if (idx >= nodes.length - 1 && !nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        return true;
      }
      // only when idx > nodes.length or nodes ends with **
      count += measurementMNode.getMeasurementCount();
    }
    return true;
  }
}
