/**
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
package org.apache.iotdb.cluster.query.reader.querynode;

import static org.apache.iotdb.cluster.query.reader.querynode.ClusterSelectSeriesBatchReader.CLUSTER_CONF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.query.common.ClusterNullableBatchData;
import org.apache.iotdb.cluster.query.utils.ClusterTimeValuePairUtils;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithOnlyTimeFilterDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;

/**
 * Batch reader entity for select paths in group by query with only time filter.
 */
public class ClusterGroupBySelectSeriesBatchReaderEntity implements
    IClusterSeriesBatchReaderEntity {

  private List<Path> paths;
  private List<TSDataType> dataTypes;

  private GroupByWithOnlyTimeFilterDataSet queryDataSet;

  public ClusterGroupBySelectSeriesBatchReaderEntity(
      List<Path> paths,
      List<TSDataType> dataTypes,
      GroupByWithOnlyTimeFilterDataSet queryDataSet) {
    this.paths = paths;
    this.dataTypes = dataTypes;
    this.queryDataSet = queryDataSet;
  }

  @Override
  public boolean hasNext() throws IOException {
    return queryDataSet.hasNext();
  }

  @Override
  public List<BatchData> nextBatchList() throws IOException {
    List<BatchData> batchDataList = new ArrayList<>(paths.size());
    for (int i = 0; i < paths.size(); i++) {
      batchDataList.add(new ClusterNullableBatchData());
    }
    int dataPointCount = 0;
    while (true) {
      if (!hasNext() || dataPointCount == CLUSTER_CONF.getBatchReadSize()) {
        break;
      }
      dataPointCount++;
      RowRecord rowRecord = queryDataSet.next();
      long time = rowRecord.getTimestamp();
      List<Field> fieldList = rowRecord.getFields();
      for (int j = 0; j < paths.size(); j++) {
        ClusterNullableBatchData batchData = (ClusterNullableBatchData) batchDataList.get(j);
        Object value = fieldList.get(j).getObjectValue(dataTypes.get(j));
        batchData.addTimeValuePair(fieldList.get(j).toString().equals("null") ? null
            : ClusterTimeValuePairUtils.getTimeValuePair(time, value, dataTypes.get(j)));
      }
    }
    return batchDataList;
  }
}
