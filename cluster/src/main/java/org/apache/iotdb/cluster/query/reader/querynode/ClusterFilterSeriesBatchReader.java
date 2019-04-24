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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * Batch reader for all filter paths.
 */
public class ClusterFilterSeriesBatchReader implements IClusterFilterSeriesBatchReader {

  private List<Path> allFilterPath;
  private List<Filter> filters;
  private QueryDataSet queryDataSet;

  public ClusterFilterSeriesBatchReader(QueryDataSet queryDataSet, List<Path> allFilterPath,
      List<Filter> filters) {
    this.queryDataSet = queryDataSet;
    this.allFilterPath = allFilterPath;
    this.filters = filters;
  }

  @Override
  public boolean hasNext() throws IOException {
    return queryDataSet.hasNext();
  }

  /**
   * Get batch data of all filter series by next batch time which is determined by
   * <code>queryDataSet</code>
   */
  @Override
  public List<BatchData> nextBatchList() throws IOException {
    List<BatchData> batchDataList = new ArrayList<>(allFilterPath.size());
    List<TSDataType> dataTypeList = queryDataSet.getDataTypes();
    for (int i = 0; i < allFilterPath.size(); i++) {
      batchDataList.add(new BatchData(dataTypeList.get(i), true));
    }
    int dataPointCount = 0;
    while(true){
      if(!hasNext() || dataPointCount == ClusterConstant.BATCH_READ_SIZE){
        break;
      }
      if(hasNext() && addTimeValuePair(batchDataList, dataTypeList)){
          dataPointCount++;
      }
    }
    return batchDataList;
  }

  /**
   * Add a time-value pair to batch data
   */
  private boolean addTimeValuePair(List<BatchData> batchDataList, List<TSDataType> dataTypeList)
      throws IOException {
    boolean hasField = false;
    RowRecord rowRecord = queryDataSet.next();
    long time = rowRecord.getTimestamp();
    List<Field> fieldList = rowRecord.getFields();
    for (int j = 0; j < allFilterPath.size(); j++) {
      if (!fieldList.get(j).isNull()) {
        BatchData batchData = batchDataList.get(j);
        Object value = fieldList.get(j).getObjectValue(dataTypeList.get(j));
        if (filters.get(j).satisfy(time, value)) {
          hasField = true;
          batchData.putTime(time);
          batchData.putAnObject(value);
        }
      }
    }
    return hasField;
  }

  public List<Path> getAllFilterPath() {
    return allFilterPath;
  }

  public void setAllFilterPath(List<Path> allFilterPath) {
    this.allFilterPath = allFilterPath;
  }

  public QueryDataSet getQueryDataSet() {
    return queryDataSet;
  }

  public void setQueryDataSet(QueryDataSet queryDataSet) {
    this.queryDataSet = queryDataSet;
  }
}
