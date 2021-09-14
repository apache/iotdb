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

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_MULTI_LEVEL_WILDCARD;

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
  protected boolean isValid(IMNode node) {
    return node.isMeasurement();
  }

  @Override
  public void processValidNode(IMNode node, int idx) throws MetadataException {
    IMeasurementSchema schema = ((IMeasurementMNode) node).getSchema();
    if (schema instanceof MeasurementSchema) {
      if (hasLimit) {
        curOffset += 1;
        if (curOffset < offset) {
          return;
        }
      }
      collectMeasurementSchema((IMeasurementMNode) node);
      if (hasLimit) {
        count += 1;
      }
    } else if (schema instanceof VectorMeasurementSchema) {
      if (idx == nodes.length
          && !nodes[nodes.length - 1].equals(PATH_MULTI_LEVEL_WILDCARD)
          && !isPrefixMatch) {
        return;
      }
      // only when idx > nodes.length or nodes ends with ** or isPrefixMatch
      List<String> measurements = schema.getSubMeasurementsList();
      for (int i = 0; i < measurements.size(); i++) {
        if (hasLimit) {
          curOffset += 1;
          if (curOffset < offset) {
            return;
          }
        }
        collectVectorMeasurementSchema((IMeasurementMNode) node, i);
        if (hasLimit) {
          count += 1;
        }
      }
    }
  }

  @Override
  protected boolean processInternalValid(IMNode node, int idx) throws MetadataException {
    if (idx == nodes.length - 1) {
      IMeasurementSchema schema = ((IMeasurementMNode) node).getSchema();
      if (schema instanceof VectorMeasurementSchema) {
        List<String> measurements = schema.getSubMeasurementsList();
        String regex = nodes[idx].replace("*", ".*");
        for (int i = 0; i < measurements.size(); i++) {
          if (!Pattern.matches(regex, measurements.get(i))) {
            continue;
          }
          if (hasLimit) {
            curOffset += 1;
            if (curOffset < offset) {
              break;
            }
          }
          collectVectorMeasurementSchema((IMeasurementMNode) node, i);
          if (hasLimit) {
            count += 1;
          }
        }
      }
    }
    return true;
  }

  protected abstract void collectMeasurementSchema(IMeasurementMNode node) throws MetadataException;

  protected abstract void collectVectorMeasurementSchema(IMeasurementMNode node, int index)
      throws MetadataException;
}
