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
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.List;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

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
    if (!node.isMeasurement() || idx != nodes.length - 2) {
      return false;
    }
    IMeasurementMNode measurementMNode = node.getAsMeasurementMNode();
    IMeasurementSchema schema = measurementMNode.getSchema();
    if (schema instanceof VectorMeasurementSchema) {
      List<String> measurements = schema.getSubMeasurementsList();
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
        collectVectorMeasurement(measurementMNode, i);
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
    IMeasurementSchema schema = measurementMNode.getSchema();
    if (schema instanceof MeasurementSchema) {
      if (hasLimit) {
        curOffset += 1;
        if (curOffset < offset) {
          return true;
        }
      }
      collectUnaryMeasurement(measurementMNode);
      if (hasLimit) {
        count += 1;
      }
    } else if (schema instanceof VectorMeasurementSchema) {
      if (idx >= nodes.length - 1
          && !nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD)
          && !isPrefixMatch) {
        return true;
      }
      // only when idx > nodes.length or nodes ends with ** or isPrefixMatch
      List<String> measurements = schema.getSubMeasurementsList();
      for (int i = 0; i < measurements.size(); i++) {
        if (hasLimit) {
          curOffset += 1;
          if (curOffset < offset) {
            return true;
          }
        }
        collectVectorMeasurement(measurementMNode, i);
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
   * @param node MeasurementMNode holding unary the measurement schema
   */
  protected abstract void collectUnaryMeasurement(IMeasurementMNode node) throws MetadataException;

  /**
   * collect the information of target sub measurement of vector measurement
   *
   * @param node MeasurementMNode holding the vector measurement schema
   * @param index the index of target sub measurement
   */
  protected abstract void collectVectorMeasurement(IMeasurementMNode node, int index)
      throws MetadataException;
}
