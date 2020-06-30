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

import static org.apache.iotdb.db.utils.QueryUtils.transferShowTimeSeriesResultToRecordList;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class ShowTimeseriesDataSet extends QueryDataSet {

  private ShowTimeSeriesPlan plan;
  private List<RowRecord> result = new ArrayList<>();
  private int index = 0;

  public ShowTimeseriesDataSet(List<Path> paths,
      List<TSDataType> dataTypes, ShowPlan showPlan) {
    super(paths, dataTypes);
    this.plan = (ShowTimeSeriesPlan) showPlan;
  }

  @Override
  protected boolean hasNextWithoutConstraint() {
    if (index == result.size()) {
      plan.setOffset(plan.getOffset() + plan.getLimit());
      try {
        List<ShowTimeSeriesResult> showTimeSeriesResults = MManager.getInstance()
            .showTimeseries(plan);
        result = transferShowTimeSeriesResultToRecordList(showTimeSeriesResults);
      } catch (Exception e) {
        e.printStackTrace();
      }
      index = 0;
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

}
