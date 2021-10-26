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
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MultiMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.UnaryMeasurementMNode;

import java.util.List;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

// This class defines MeasurementMNode as target node and
// defines the measurement component process framework.
// UnaryMeasurement and each component of MultiMeasurement will be processed.
public abstract class FlatMeasurementCollector<T> extends CollectorTraverser<T> {

  public FlatMeasurementCollector(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
    isMeasurementTraverser = true;
  }

  public FlatMeasurementCollector(IMNode startNode, PartialPath path, int limit, int offset)
      throws MetadataException {
    super(startNode, path, limit, offset);
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
      MultiMeasurementMNode multiMeasurementMNode = measurementMNode.getAsMultiMeasurementMNode();
      List<String> measurements = multiMeasurementMNode.getSubMeasurementList();
      String targetNameRegex = nodes[idx + 1].replace("*", ".*");
      for (int i = 0; i < measurements.size(); i++) {
        if (!Pattern.matches(targetNameRegex, measurements.get(i))) {
          continue;
        }
        if (hasLimit) {
          curOffset += 1;
          if (curOffset < offset) {
            break;
          }
        }
        collectMultiMeasurementComponent(multiMeasurementMNode, i);
        if (hasLimit) {
          count += 1;
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
      if (hasLimit) {
        curOffset += 1;
        if (curOffset < offset) {
          return true;
        }
      }
      collectUnaryMeasurement(measurementMNode.getAsUnaryMeasurementMNode());
      if (hasLimit) {
        count += 1;
      }
    } else if (measurementMNode.isMultiMeasurement()) {
      if (idx >= nodes.length - 1
          && !nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD)
          && !isPrefixMatch) {
        return true;
      }
      MultiMeasurementMNode multiMeasurementMNode = measurementMNode.getAsMultiMeasurementMNode();
      // only when idx > nodes.length or nodes ends with ** or isPrefixMatch
      List<String> measurements = multiMeasurementMNode.getSubMeasurementList();
      for (int i = 0; i < measurements.size(); i++) {
        if (hasLimit) {
          curOffset += 1;
          if (curOffset < offset) {
            return true;
          }
        }
        collectMultiMeasurementComponent(multiMeasurementMNode, i);
        if (hasLimit) {
          count += 1;
        }
      }
    }
    return true;
  }

  /**
   * collect the information of unary measurement
   *
   * @param node MeasurementMNode holding the unary measurement schema
   */
  protected abstract void collectUnaryMeasurement(UnaryMeasurementMNode node)
      throws MetadataException;

  /**
   * collect the information of target sub measurement of vector measurement
   *
   * @param node MeasurementMNode holding the vector measurement schema
   * @param index the index of target sub measurement
   */
  protected abstract void collectMultiMeasurementComponent(MultiMeasurementMNode node, int index)
      throws MetadataException;
}
