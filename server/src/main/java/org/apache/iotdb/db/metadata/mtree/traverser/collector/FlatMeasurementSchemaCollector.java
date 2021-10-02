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
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MultiMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.UnaryMeasurementMNode;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.LinkedList;
import java.util.List;

import static org.apache.iotdb.db.metadata.lastCache.LastCacheManager.getLastTimeStamp;

// This class implements the measurement collection function.
public class FlatMeasurementSchemaCollector
    extends FlatMeasurementCollector<List<Pair<PartialPath, String[]>>> {

  // whether show timeseries with last value
  protected boolean needLast = false;
  // queryContext helps get last value
  protected QueryContext queryContext;

  public FlatMeasurementSchemaCollector(IMNode startNode, PartialPath path)
      throws MetadataException {
    super(startNode, path);
    this.resultSet = new LinkedList<>();
  }

  public FlatMeasurementSchemaCollector(IMNode startNode, PartialPath path, int limit, int offset)
      throws MetadataException {
    super(startNode, path, limit, offset);
    this.resultSet = new LinkedList<>();
  }

  public void setNeedLast(boolean needLast) {
    this.needLast = needLast;
  }

  public void setQueryContext(QueryContext queryContext) {
    this.queryContext = queryContext;
  }

  @Override
  protected void collectUnaryMeasurement(UnaryMeasurementMNode node) throws MetadataException {
    IMeasurementSchema measurementSchema = node.getSchema();
    String[] tsRow = new String[7];
    tsRow[0] = node.getAlias();
    tsRow[1] = getStorageGroupPath(node).getFullPath();
    tsRow[2] = measurementSchema.getType().toString();
    tsRow[3] = measurementSchema.getEncodingType().toString();
    tsRow[4] = measurementSchema.getCompressor().toString();
    tsRow[5] = String.valueOf(node.getOffset());
    tsRow[6] = needLast ? String.valueOf(getLastTimeStamp(node, queryContext)) : null;
    Pair<PartialPath, String[]> temp = new Pair<>(node.getPartialPath(), tsRow);
    resultSet.add(temp);
  }

  @Override
  protected void collectMultiMeasurementComponent(MultiMeasurementMNode node, int index)
      throws MetadataException {
    IMeasurementSchema schema = node.getSchema();
    List<String> measurements = schema.getSubMeasurementsList();
    String[] tsRow = new String[7];
    tsRow[0] = null;
    tsRow[1] = getStorageGroupPath(node).getFullPath();
    tsRow[2] = schema.getSubMeasurementsTSDataTypeList().get(index).toString();
    tsRow[3] = schema.getSubMeasurementsTSEncodingList().get(index).toString();
    tsRow[4] = schema.getCompressor().toString();
    tsRow[5] = "-1";
    tsRow[6] = needLast ? String.valueOf(getLastTimeStamp(node, queryContext)) : null;
    Pair<PartialPath, String[]> temp =
        new Pair<>(new VectorPartialPath(node.getFullPath(), measurements.get(index)), tsRow);
    resultSet.add(temp);
  }

  private PartialPath getStorageGroupPath(IMeasurementMNode node)
      throws StorageGroupNotSetException {
    if (node == null) {
      return null;
    }
    IMNode temp = node;
    while (temp != null) {
      if (temp.isStorageGroup()) {
        break;
      }
      temp = temp.getParent();
    }
    if (temp == null) {
      throw new StorageGroupNotSetException(node.getFullPath());
    }
    return temp.getPartialPath();
  }
}
