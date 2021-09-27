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
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

// This class implements related entity paths collection for given timeseries path pattern.
// All the entities, that one of the timeseries matching the path pattern belongs to, will be
// collected.
public class TSEntityPathCollector extends CollectorTraverser<Set<PartialPath>> {

  public TSEntityPathCollector(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
    this.resultSet = new TreeSet<>();
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
    if (!node.isMeasurement() || idx != nodes.length - 2) {
      return false;
    }
    IMeasurementSchema schema = node.getAsMeasurementMNode().getSchema();
    if (schema instanceof VectorMeasurementSchema) {
      List<String> measurements = schema.getSubMeasurementsList();
      String regex = nodes[idx + 1].replace("*", ".*");
      for (String measurement : measurements) {
        if (!Pattern.matches(regex, measurement)) {
          resultSet.add(node.getParent().getPartialPath());
          break;
        }
      }
    }
    return true;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
    if (!node.isMeasurement()) {
      return false;
    }
    IMeasurementSchema schema = node.getAsMeasurementMNode().getSchema();
    if (schema instanceof MeasurementSchema) {
      resultSet.add(node.getParent().getPartialPath());
    } else if (schema instanceof VectorMeasurementSchema) {
      if (idx >= nodes.length - 1
          && !nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD)
          && !isPrefixMatch) {
        return true;
      }
      // only when idx > nodes.length or nodes ends with ** or isPrefixMatch
      resultSet.add(node.getParent().getPartialPath());
    }
    return true;
  }
}
