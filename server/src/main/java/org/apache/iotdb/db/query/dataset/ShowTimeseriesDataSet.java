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

package org.apache.iotdb.db.query.dataset;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_ALIAS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

public class ShowTimeseriesDataSet extends QueryDataSet {

  private ShowTimeSeriesPlan plan;
  private List<RowRecord> result = new ArrayList<>();
  private int index = 0;

  public ShowTimeseriesDataSet(List<Path> paths,
      List<TSDataType> dataTypes, ShowPlan showPlan) {
    super(paths, dataTypes);
    this.plan = (ShowTimeSeriesPlan)showPlan;
  }

  @Override
  protected boolean hasNextWithoutConstraint() {
    if(index==result.size()){
      plan.setOffset(plan.getOffset()+plan.getLimit());
      try{
        List<ShowTimeSeriesResult> showTimeSeriesResults = MManager.getInstance().showTimeseries(plan);
        result = transferShowTimeSeriesResultToRecordList(showTimeSeriesResults,plan);
      }catch (Exception e){
        e.printStackTrace();
      }
    }
    return index < result.size();
  }

  @Override
  protected RowRecord nextWithoutConstraint() {
    return result.get(index++);
  }

  public void putRecord(RowRecord newRecord) {
    result.add(newRecord);
  }

  public List<RowRecord> transferShowTimeSeriesResultToRecordList(
      List<ShowTimeSeriesResult> timeseriesList, ShowTimeSeriesPlan showTimeSeriesPlan) {
    List<RowRecord> records = new ArrayList<>();
    List<Path> paths = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    paths.add(new Path(COLUMN_TIMESERIES));
    dataTypes.add(TSDataType.TEXT);
    paths.add(new Path(COLUMN_TIMESERIES_ALIAS));
    dataTypes.add(TSDataType.TEXT);
    paths.add(new Path(COLUMN_STORAGE_GROUP));
    dataTypes.add(TSDataType.TEXT);
    paths.add(new Path(COLUMN_TIMESERIES_DATATYPE));
    dataTypes.add(TSDataType.TEXT);
    paths.add(new Path(COLUMN_TIMESERIES_ENCODING));
    dataTypes.add(TSDataType.TEXT);
    paths.add(new Path(COLUMN_TIMESERIES_COMPRESSION));
    dataTypes.add(TSDataType.TEXT);
    Set<String> tagAndAttributeName = new TreeSet<>();
    for (ShowTimeSeriesResult result : timeseriesList) {
      tagAndAttributeName.addAll(result.getTagAndAttribute().keySet());
    }
    for (String key : tagAndAttributeName) {
      paths.add(new Path(key));
      dataTypes.add(TSDataType.TEXT);
    }

    for (ShowTimeSeriesResult result : timeseriesList) {
      RowRecord record = new RowRecord(0);
      updateRecord(record, result.getName());
      updateRecord(record, result.getAlias());
      updateRecord(record, result.getSgName());
      updateRecord(record, result.getDataType());
      updateRecord(record, result.getEncoding());
      updateRecord(record, result.getCompressor());
      updateRecord(record, result.getTagAndAttribute(), paths);
      records.add(record);
    }
    return records;
  }

  private void updateRecord(
      RowRecord record, Map<String, String> tagAndAttribute, List<Path> paths) {
    for (int i = 6; i < paths.size(); i++) {
      updateRecord(record, tagAndAttribute.get(paths.get(i).getFullPath()));
    }
  }

  private void updateRecord(RowRecord record, String s) {
    if (s == null) {
      record.addField(null);
      return;
    }
    Field field = new Field(TSDataType.TEXT);
    field.setBinaryV(new Binary(s));
    record.addField(field);
  }
}